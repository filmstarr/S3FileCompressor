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

        IAmazonS3 S3Client { get; set; }

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
                context.Logger.LogLine("Starting function");
                var response = await this.S3Client.GetObjectMetadataAsync(s3Event.Bucket.Name, s3Event.Object.Key);
                context.Logger.LogLine("Really Starting function");
                await CompressFile(s3Event.Bucket.Name, s3Event.Object.Key);
                return response.Headers.ContentType;
            }
            catch(Exception e)
            {
                context.Logger.LogLine($"Error getting object {s3Event.Object.Key} from bucket {s3Event.Bucket.Name}. Make sure they exist and your bucket is in the same region as this function.");
                context.Logger.LogLine(e.Message);
                context.Logger.LogLine(e.StackTrace);
                throw;
            }
        }

        public async Task CompressFile(string bucketName, string key)
        {
            if (key.EndsWith(_ZippedFileExtension))
            {
                Console.WriteLine($"File already appears to be zipped, file extension is: {_ZippedFileExtension}. Function complete.");
                return;
            }

            var zippedKey = key + _ZippedFileExtension;

            var obj = await S3Client.GetObjectAsync(new GetObjectRequest { BucketName = bucketName, Key = key });

            var inputStream = obj.ResponseStream;

            var MB = (int)Math.Pow(2, 20);

            // Initiate multipart upload
            InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest { BucketName = bucketName, Key = zippedKey };
            InitiateMultipartUploadResponse initResponse = await S3Client.InitiateMultipartUploadAsync(initRequest);

            var uploadPartResponses = new List<PartETag>();

            var partNumber = 1;

            var readChunkSize = 100 * MB;
            var minUploadSize = 100 * MB;

            var streamEnded = false;
            while (!streamEnded)
            {
                using (MemoryStream memoryStream = new MemoryStream())
                {
                    using (GZipStream zipStream = new GZipStream(memoryStream, CompressionLevel.Fastest, true))
                    {
                        while (memoryStream.Length < minUploadSize)
                        {
                            var bytesCopied = CopyBytes(readChunkSize, inputStream, zipStream);
                            if (bytesCopied == 0)
                            {
                                streamEnded = true;
                                break;
                            }
                        }
                    }

                    if (memoryStream.Length == 0)
                    {
                        break;
                    }

                    memoryStream.Position = 0;

                    UploadPartRequest uploadRequest;
                    //Upload part
                    if (!streamEnded)
                    {
                        uploadRequest = new UploadPartRequest
                        {
                            BucketName = bucketName,
                            Key = zippedKey,
                            UploadId = initResponse.UploadId,
                            PartNumber = partNumber,
                            PartSize = memoryStream.Length,
                            InputStream = memoryStream
                        };
                    }
                    else
                    {
                        uploadRequest = new UploadPartRequest
                        {
                            BucketName = bucketName,
                            Key = zippedKey,
                            UploadId = initResponse.UploadId,
                            PartNumber = partNumber,
                            InputStream = memoryStream,
                        };

                    }

                    UploadPartResponse uploadPartResponse = await S3Client.UploadPartAsync(uploadRequest);
                    uploadPartResponses.Add(new PartETag { ETag = uploadPartResponse.ETag, PartNumber = partNumber });

                    partNumber++;
                }
            }

            CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest
            {
                BucketName = bucketName,
                Key = zippedKey,
                UploadId = initResponse.UploadId,
                PartETags = uploadPartResponses
            };
            CompleteMultipartUploadResponse compResponse = await S3Client.CompleteMultipartUploadAsync(compRequest);

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
    }
}
