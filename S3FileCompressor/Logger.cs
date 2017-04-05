using Amazon.Lambda.Core;
using System;

namespace S3FileCompressor
{
    internal class Logger
    {
        internal ILambdaContext Context { get; set; }

        internal void LogLine(string message)
        {
            if (this.Context?.Logger != null)
            {
                this.Context.Logger.LogLine($"{message}");
                return;
            }
            Console.WriteLine($"{message}");
        }

        internal void LogLineWithId(string message)
        {
            if (this.Context != null)
            {
                if (this.Context.Logger != null)
                {
                    this.Context.Logger.LogLine($"{this.Context.AwsRequestId}: {message}");
                    return;
                }
                Console.WriteLine($"{this.Context.AwsRequestId}: {message}");
                return;
            }
            Console.WriteLine($"{message}");
        }
    }
}
