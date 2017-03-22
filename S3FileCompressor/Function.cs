using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Model;
using System.IO;
using System.IO.Compression;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace S3FileCompressor
{
    public class Function
    {
        private const string _ZippedFileExtension = ".gz";
        private const string _FilePartReadSizeVariableName = "FilePartReadSizeMB";
        private const string _MinimumUploadSizeVariableName = "MinimumUploadSizeMB";

        private readonly int MB = (int)Math.Pow(2, 20);
        private readonly int DefaultFilePartReadSizeMB = 100;
        private readonly int DefaultMinimumUploadSizeMB = 100;

        private int FilePartReadSize
        {
            get
            {
                var environmentVariableString = Environment.GetEnvironmentVariable(_FilePartReadSizeVariableName);
                if (int.TryParse(environmentVariableString, out int value))
                {
                    return value * MB;
                }
                return this.DefaultFilePartReadSizeMB * MB;
            }
        }

        private int MinimumUploadSize
        {
            get
            {
                var environmentVariableString = Environment.GetEnvironmentVariable(_MinimumUploadSizeVariableName);
                if (int.TryParse(environmentVariableString, out int value))
                {
                    return Math.Max(value, 5) * MB;
                }
                return this.DefaultMinimumUploadSizeMB * MB;
            }
        }


        private IAmazonS3 S3Client { get; set; }

        /// <summary>
        /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
        /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        /// region the Lambda function is executed in.
        /// </summary>
        public Function()
        {
            S3Client = new AmazonS3Client();
        }

        /// <summary>
        /// Constructs an instance with a preconfigured S3 client. This can be used for testing the outside of the Lambda environment.
        /// </summary>
        /// <param name="s3Client"></param>
        public Function(IAmazonS3 s3Client)
        {
            this.S3Client = s3Client;
        }
        
        /// <summary>
        /// This method is called for every Lambda invocation. This method takes in an S3 event object and can be used 
        /// to respond to S3 notifications.
        /// </summary>
        /// <param name="evnt"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task<string> FunctionHandler(S3Event evnt, ILambdaContext context)
        {
            var s3Event = evnt.Records?[0].S3;
            if(s3Event == null)
            {
                return null;
            }

            try
            {
                context.LogLineWithId($"S3 event received. Compressing object: {s3Event.Bucket.Name}/{s3Event.Object.Key}");
                var response = await this.S3Client.GetObjectMetadataAsync(s3Event.Bucket.Name, s3Event.Object.Key);
                await CompressFile(context, s3Event.Bucket.Name, s3Event.Object.Key);
                return response.Headers.ContentType;
            }
            catch(Exception e)
            {
                context.LogLineWithId($"Error compressing object {s3Event.Object.Key} in bucket {s3Event.Bucket.Name}");
                context.Logger.LogLine(e.Message);
                context.Logger.LogLine(e.StackTrace);
                throw;
            }
        }

        private static void Log(ILambdaContext context, string message)
        {
            context.Logger.LogLine($"{message} RequestId: {context.AwsRequestId}");
        }

        private async Task CompressFile(ILambdaContext context, string bucketName, string key)
        {
            //Check if file is already zipped
            if (key.EndsWith(_ZippedFileExtension))
            {
                context.LogLineWithId($"File already appears to be zipped, file extension is: {_ZippedFileExtension}. Function complete");
                return;
            }

            //Retrieve input stream
            var obj = await S3Client.GetObjectAsync(new GetObjectRequest { BucketName = bucketName, Key = key });
            var inputStream = obj.ResponseStream;
            context.LogLineWithId("Object response stream obtained");

            //Initiate multipart upload
            var zippedKey = key + _ZippedFileExtension;
            InitiateMultipartUploadRequest uploadRequest = new InitiateMultipartUploadRequest { BucketName = bucketName, Key = zippedKey };
            InitiateMultipartUploadResponse uploadResponse = await S3Client.InitiateMultipartUploadAsync(uploadRequest);
            var uploadPartResponses = new List<PartETag>();
            context.LogLineWithId("Upload initiated");

            var partNumber = 1;
            var filePartReadSize = this.FilePartReadSize;
            var minimumUploadSize = this.MinimumUploadSize;
            var streamEnded = false;

            //Process input file
            while (!streamEnded)
            {
                using (MemoryStream memoryStream = new MemoryStream())
                {
                    //Read chunks of the input file, compressing as we go, until we hit the minimum upload size or the end of the stream
                    using (GZipStream zipStream = new GZipStream(memoryStream, CompressionLevel.Fastest, true))
                    {
                        while (memoryStream.Length < minimumUploadSize)
                        {
                            //Copy a set number of bytes from the input stream to the compressed memory stream
                            var bytesCopied = CopyBytes(filePartReadSize, inputStream, zipStream);
                            if (bytesCopied == 0)
                            {
                                streamEnded = true;
                                break;
                            }
                        }
                    }
                    context.LogLineWithId($"Part {partNumber} read and compressed");

                    //Check to make sure that we have actually read something in before proceeding
                    if (memoryStream.Length == 0)
                    {
                        break;
                    }

                    //Upload memory stream to S3
                    memoryStream.Position = 0;
                    UploadPartRequest uploadPartRequest = GetUploadPartRequest(bucketName, zippedKey, uploadResponse.UploadId, partNumber, streamEnded, memoryStream);
                    UploadPartResponse uploadPartResponse = await S3Client.UploadPartAsync(uploadPartRequest);
                    uploadPartResponses.Add(new PartETag { ETag = uploadPartResponse.ETag, PartNumber = partNumber });
                    context.LogLineWithId($"Part {partNumber} uploaded.");

                    partNumber++;
                }
            }

            //Complete multipart upload request
            context.LogLineWithId($"Completing upload request");
            CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest
            {
                BucketName = bucketName,
                Key = zippedKey,
                UploadId = uploadResponse.UploadId,
                PartETags = uploadPartResponses
            };
            CompleteMultipartUploadResponse compResponse = await S3Client.CompleteMultipartUploadAsync(compRequest);
            context.LogLineWithId($"Upload request completed");

            inputStream.Dispose();
        }

        private static long CopyBytes(long bytesToRead, Stream fromStream, Stream toStream)
        {
            var buffer = new byte[81920];
            long bytesRead = 0;
            int read;
            while ((read = fromStream.Read(buffer, 0, (int)Math.Min((bytesToRead - bytesRead), buffer.Length))) != 0)
            {
                bytesRead += read;
                toStream.Write(buffer, 0, read);
            }
            return bytesRead;
        }

        private static UploadPartRequest GetUploadPartRequest(string bucketName, string zippedKey, string uploadId, int partNumber, bool lastFilePart, MemoryStream inputStream)
        {
            UploadPartRequest uploadPartRequest;
            if (!lastFilePart)
            {
                uploadPartRequest = new UploadPartRequest
                {
                    BucketName = bucketName,
                    Key = zippedKey,
                    UploadId = uploadId,
                    PartNumber = partNumber,
                    PartSize = inputStream.Length,
                    InputStream = inputStream
                };
            }
            else
            {
                uploadPartRequest = new UploadPartRequest
                {
                    BucketName = bucketName,
                    Key = zippedKey,
                    UploadId = uploadId,
                    PartNumber = partNumber,
                    InputStream = inputStream,
                };

            }

            return uploadPartRequest;
        }
    }
}
