# OpenStack to S3 Transfer Script

## Overview

This Python script provides functionality to transfer objects from an OpenStack container to an AWS S3 bucket. It downloads each object from the specified OpenStack container, and uploads it to the given S3 bucket, creating any necessary directory structures in the process. The script uses multi-threading to expedite the transfer and includes error handling to retry failed uploads.

## Features

- **Multi-Threaded Transfer**: Utilizes a thread pool to concurrently upload multiple objects.
- **MD5 Checksum Verification**: Ensures that files are only uploaded if they have been modified or do not exist in S3.
- **Temporary Storage**: Downloads are saved in a specified temporary directory which is deleted after the transfer is complete.
- **Logging**: Provides detailed logging to both the console and a log file, with a format including a timestamp, logger name, and message.
- **Retries for Failures**: Includes retry logic for failed S3 uploads, ensuring greater reliability.

## Prerequisites

- **Python 3.x**
- Required Python packages:
  - `boto3`: AWS SDK for Python to handle S3 operations.
  - `swiftclient`: Client library for OpenStack Swift.
  - `keystoneauth1`: Handles authentication for OpenStack.
- AWS credentials need to be configured as environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`).
- OpenStack credentials need to be configured as environment variables (`OS_AUTH_URL`, `OS_APPLICATION_CREDENTIAL_ID`, `OS_APPLICATION_CREDENTIAL_SECRET`).

## Installation

1. **Create a Virtual Environment**

   Create a virtual environment to manage dependencies for this script:

   ```sh
   python3 -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

2. **Install Required Packages**

   Install the required Python packages using `pip` and `requirements.txt`:

   ```sh
   pip install -r requirements.txt
   ```

## Usage

Run the script using the command line with the required arguments:

```sh
python3 script.py --openStackContainer <OpenStackContainerName> --s3Bucket <S3BucketName> \
  --maxWorkers <MaxWorkers> --regionName <AWSRegion> --bandwidthLimitMb <BandwidthLimitMb>
```

### Arguments

- `--openStackContainer`: Name of the OpenStack container.
- `--s3Bucket`: Name of the target S3 bucket.
- `--maxWorkers`: Number of workers for concurrent uploads (Minimum: 1).
- `--regionName`: AWS region name for the target S3 bucket.
- `--bandwidthLimitMb`: Maximum bandwidth limit for S3 uploads in MB (Minimum: 1).

### Example

```sh
python3 script.py --openStackContainer oss_container_name --s3Bucket s3_bucket_name \
  --maxWorkers 10 --regionName ap-southeast-1 --bandwidthLimitMb 1
```

## Logging

The script logs the entire process, including success and error messages, both to the console and a log file. The log file is named using the format:

```
YYYY-MM-DD-_%H-%M-%S_{openStackContainer}_to_{s3Bucket}.log
```

The log includes details like:
- Start and end of the transfer.
- Object creation in the S3 bucket.
- Upload attempts and retries.
- Comparison of the number of objects between the source and target.

## Script Flow

1. **Logger Creation**: A logger is created with a timestamped log file and console output.
2. **Session Setup**: An OpenStack session and an S3 client are created based on the provided environment variables.
3. **Temporary Directory**: A temporary directory is created to store downloaded files.
4. **Listing Objects**: Lists all objects in the OpenStack container and checks the existence of the target S3 bucket.
5. **Multi-Threaded Transfer**: Objects are downloaded from OpenStack and uploaded to S3 concurrently.
6. **Verification**: After transfer, a comparison of the total number of objects between OpenStack and S3 is performed.
7. **Cleanup**: Temporary files are deleted, and logs indicate the completion of the process.

## Error Handling

- **Bucket Verification**: Ensures the target S3 bucket exists before proceeding.
- **Retries**: Failed uploads are retried with an exponential backoff.
- **Expired AWS Credentials**: The script prompts for refreshed AWS credentials if the current session expires.

## Requirements for Running the Script

Ensure that the following environment variables are set correctly for both AWS and OpenStack credentials:

### AWS Environment Variables
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_SESSION_TOKEN`

### OpenStack Environment Variables
- `OS_AUTH_URL`
- `OS_APPLICATION_CREDENTIAL_ID`
- `OS_APPLICATION_CREDENTIAL_SECRET`

## Important Considerations

- **Temporary Directory Storage**: The script downloads each object to a specified temporary directory. Ensure sufficient disk space is available for the downloaded data.
- **Thread Pool Size**: The `--maxWorkers` parameter controls the number of concurrent threads for upload. Adjust this based on your system's capacity.
- **Bandwidth Limitation**: The `--bandwidthLimitMb` helps in managing the upload speed to prevent network congestion.

## License

This script is provided "as-is" without warranty of any kind, express or implied. Please review and adjust the script as per your requirements.

## Contact

If you have any questions or issues, feel free to reach out to the script author or open an issue in your repository. Contributions and suggestions for improvement are welcome.

