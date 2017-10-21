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

        private readonly Variables variables = new Variables();
        private readonly Logger logger = new Logger();

        private IAmazonS3 S3Client { get; set; }

        /// <summary>
        /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
        /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        /// region the Lambda function is executed in.
        /// </summary>
        public Function()
        {
            this.S3Client = new AmazonS3Client();
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
            //Set logging context
            this.logger.Context = context;

            var s3Event = evnt.Records?[0].S3;
            if(s3Event == null)
            {
                return null;
            }

            //Decode object key from event
            var s3EventObjectKey = Uri.UnescapeDataString(s3Event.Object.Key);

            try
            {
                logger.LogLineWithId($"S3 event received. Compressing object: {s3Event.Bucket.Name}/{s3EventObjectKey}");
                var response = await this.S3Client.GetObjectMetadataAsync(s3Event.Bucket.Name, s3EventObjectKey);
                await CompressFile(context, s3Event.Bucket.Name, s3EventObjectKey);
                return response.Headers.ContentType;
            }
            catch(Exception e)
            {
                logger.LogLineWithId($"Error compressing object {s3EventObjectKey} in bucket {s3Event.Bucket.Name}");
                context.Logger.LogLine(e.Message);
                context.Logger.LogLine(e.StackTrace);
                throw;
            }
        }

        private async Task CompressFile(ILambdaContext context, string inputBucketName, string inputKey)
        {
            //Check if file is already zipped
            if (inputKey.EndsWith(_ZippedFileExtension))
            {
                logger.LogLineWithId($"File already appears to be zipped, file extension is: {_ZippedFileExtension}. Function complete");
                return;
            }

            //Retrieve input stream
            var obj = await S3Client.GetObjectAsync(new GetObjectRequest { BucketName = inputBucketName, Key = inputKey });
            var inputStream = obj.ResponseStream;
            logger.LogLineWithId("Object response stream obtained");

            //Initiate multipart upload
            string outputBucketName = !string.IsNullOrEmpty(this.variables.OutputBucket) ? this.variables.OutputBucket : inputBucketName;
            string outputKey = this.GetOutputKey(inputKey);
            InitiateMultipartUploadRequest uploadRequest = new InitiateMultipartUploadRequest { BucketName = outputBucketName, Key = outputKey };
            InitiateMultipartUploadResponse uploadResponse = await S3Client.InitiateMultipartUploadAsync(uploadRequest);
            var uploadPartResponses = new List<PartETag>();
            logger.LogLineWithId($"Upload initiated. Output object: {outputBucketName}/{outputKey}");

            var partNumber = 1;
            var filePartReadSize = this.variables.FilePartReadSize;
            var minimumUploadSize = this.variables.MinimumUploadSize;
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
                    logger.LogLineWithId($"Part {partNumber} read and compressed");

                    //Check to make sure that we have actually read something in before proceeding
                    if (memoryStream.Length == 0)
                    {
                        break;
                    }

                    //Upload memory stream to S3
                    memoryStream.Position = 0;
                    UploadPartRequest uploadPartRequest = GetUploadPartRequest(outputBucketName, outputKey, uploadResponse.UploadId, partNumber, streamEnded, memoryStream);
                    UploadPartResponse uploadPartResponse = await S3Client.UploadPartAsync(uploadPartRequest);
                    uploadPartResponses.Add(new PartETag { ETag = uploadPartResponse.ETag, PartNumber = partNumber });
                    logger.LogLineWithId($"Part {partNumber} uploaded.");

                    partNumber++;
                }

                //Run the garbage collector so that Lambda doesn't run out of memory for large files
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }

            //Complete multipart upload request
            logger.LogLineWithId($"Completing upload request");
            CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest
            {
                BucketName = outputBucketName,
                Key = outputKey,
                UploadId = uploadResponse.UploadId,
                PartETags = uploadPartResponses
            };
            CompleteMultipartUploadResponse compResponse = await S3Client.CompleteMultipartUploadAsync(compRequest);
            logger.LogLineWithId($"Upload request completed");

            inputStream.Dispose();
        }

        private string GetOutputKey(string key)
        {
            var outputKey = key + _ZippedFileExtension;

            if (this.variables.FlattenFilePaths)
            {
                outputKey = outputKey.Substring(outputKey.IndexOf('/') + 1);
            }
            return this.variables.OutputFolderPath + outputKey;
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
