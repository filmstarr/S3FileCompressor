# S3FileCompressor

A .NET Core, AWS Lambda function to compress files as they are uploaded to an S3 bucket.

This AWS Lambda function can be configured to listen to S3 object creation events (Put, Post, Copy, Complete Multipart Upload) and will re-upload a gzipped version of the file to the same bucket location.

S3FileCompressor has been build to memory efficient, streaming, compressing and writing data back to S3 via multipart upload in continuous process. As such, large files can be compressed without the need to read the whole file into memory, and memory usage can be tightly constrained.

## Prerequisits & Configuration
The prerequisits and configuration of this Lambda function are no different to any other. This project contains a set of default configuration values in the 
[aws-lambda-tools-defaults.json file](S3FileCompressor/aws-lambda-tools-defaults.json), which define the region the S3FileCompressor function is to run in, the length of time that the function can run for, the maximum memory that it can use and the IAM role under which the function is to run. These should be changed to suitable values for your requirements and production enviroment.

The IAM role used will need to have a policy which can get/put objects from/to the S3 bucket(s) with files that you wish to compress as well as suitable CloudWatch permissions. The AWSLambdaExecute managed policy is a good place to start:

```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "logs:*"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::*"
    }
  ]
}
```


## Deployment

Deployment of the S3FileCompressor function can be done through the AWS CLI, the AWS Toolkit for Visual Studio or through the AWS web interface. There are again no special requirements for this function compared to any other.

Once the Lambda function has been created the trigger needs to be configured, either via the AWS S3 CLI or through the web interface. Specify the bucket you want the function to apply to, the event types to trigger the function (e.g. "Object Created (All)") and any prefix or suffix filters.


## Function Variables

S3FileCompressor has two variables that can be set when deploying the function:
* FilePartReadSizeMB - the part size that is read from the input file before compression (multiple parts may be read and then compressed before uploading to S3)
* MinimumUploadSizeMB - the minimum size of file part that will be uploaded back to S3 after compression (this controls how of the output file is held in memory before being flushed to S3)