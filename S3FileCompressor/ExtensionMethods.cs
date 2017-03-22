using System;

using Amazon.Lambda.Core;

namespace S3FileCompressor
{
    internal static class ExtensionMethods
    {
        internal static void LogLineWithId(this ILambdaContext context, string message)
        {
            if (context.Logger != null)
            {
                context.Logger.LogLine($"{context.AwsRequestId}: {message}");
            }
            else
            {
                Console.WriteLine($"{context.AwsRequestId}: {message}");
            }

        }
    }
}
