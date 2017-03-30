﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace S3FileCompressor
{
    public class S3FileCompressorVariables
    {
        private const string _ZippedFileExtension = ".gz";
        private const string _FilePartReadSizeVariableName = "FilePartReadSizeMB";
        private const string _FlattenFilePathsVariableName = "FlattenFilePaths";
        private const string _MinimumUploadSizeVariableName = "MinimumUploadSizeMB";
        private const string _OutputBucketVariableName = "OutputBucket";
        private const string _OutputFolderPathVariableName = "OutputFolderPath";

        private const int DefaultFilePartReadSizeMB = 100;
        private const int DefaultMinimumUploadSizeMB = 100;

        private readonly IDictionary EnvironmentVariables = Environment.GetEnvironmentVariables();
        private readonly int MB = (int)Math.Pow(2, 20);

        public int FilePartReadSize
        {
            get
            {
                var environmentVariableString = this.GetEnvironmentVariableString(_FilePartReadSizeVariableName);
                if (int.TryParse(environmentVariableString, out int value))
                {
                    return Math.Max(value, 5) * MB;
                }
                return DefaultFilePartReadSizeMB * MB;
            }
        }

        public bool FlattenFilePaths
        {
            get
            {
                var environmentVariableString = this.GetEnvironmentVariableString(_FlattenFilePathsVariableName);
                if (bool.TryParse(environmentVariableString, out bool value))
                {
                    return value;
                }
                return false;
            }
        }

        public int MinimumUploadSize
        {
            get
            {
                var environmentVariableString = this.GetEnvironmentVariableString(_MinimumUploadSizeVariableName);
                if (int.TryParse(environmentVariableString, out int value))
                {
                    return Math.Max(value, 5) * MB;
                }
                return DefaultMinimumUploadSizeMB * MB;
            }
        }

        public string OutputBucket
        {
            get
            {
                var environmentVariableString = this.GetEnvironmentVariableString(_OutputBucketVariableName);
                if (!string.IsNullOrEmpty(environmentVariableString))
                {
                    return environmentVariableString;
                }
                return "";
            }
        }

        public string OutputFolderPath
        {
            get
            {
                var environmentVariableString = this.GetEnvironmentVariableString(_OutputFolderPathVariableName);
                if (!string.IsNullOrEmpty(environmentVariableString))
                {
                    var outputfolderPath = environmentVariableString.Replace('\\', '/').TrimStart('/').TrimEnd('/');
                    return outputfolderPath + "/";
                }
                return "";
            }
        }

        private string GetEnvironmentVariableString(string key)
        {
            //For some reason Environment.GetEnvironmentVariable is returning random data when variables are empty.
            //Work around this by using Environment.GetEnvironmentVariables instead.
            if (this.EnvironmentVariables.Contains(key))
            {
                var environmentVariable = this.EnvironmentVariables[key].ToString();
                if (!string.IsNullOrWhiteSpace(environmentVariable))
                {
                    return environmentVariable;
                }
            }
            return null;
        }
    }
}
