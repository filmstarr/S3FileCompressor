using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Xunit;
using Amazon.Lambda.S3Events;

using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Util;
using static Amazon.Lambda.S3Events.S3Event;

namespace S3FileCompressor.Tests
{
    public class FunctionTest
    {
        [Fact]
        public async Task TestS3EventLambdaFunction()
        {
            IAmazonS3 s3Client = new AmazonS3Client(RegionEndpoint.USWest2);

            var bucketName = "lambda-S3FileCompressor-".ToLower() + DateTime.Now.Ticks;
            var key = "text.txt";

            // Create a bucket an object to setup a test data.
            await s3Client.PutBucketAsync(bucketName);
            try
            {
                await s3Client.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    ContentBody = "sample data"
                });

                // Setup the S3 event object that S3 notifications would create with the fields used by the Lambda function.
                var s3Event = new S3Event
                {
                    Records = new List<S3EventNotificationRecord>
                    {
                        new S3EventNotificationRecord
                        {
                            S3 = new S3Entity
                            {
                                Bucket = new S3BucketEntity {Name = bucketName },
                                Object = new S3ObjectEntity {Key = key }
                            }
                        }
                    }
                };

                // Invoke the lambda function and confirm the content type was returned.
                var function = new Function(s3Client);
                var contentType = await function.FunctionHandler(s3Event, null);

                Assert.Equal("text/plain", contentType);

            }
            finally
            {
                // Clean up the test data
                await AmazonS3Util.DeleteS3BucketWithObjectsAsync(s3Client, bucketName);
            }
        }
    }
}
