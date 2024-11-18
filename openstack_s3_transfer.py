import os
import shutil
import time
import logging
import hashlib
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
import swiftclient
from keystoneauth1 import session
from keystoneauth1.identity import v3 as auth_v3

def create_logger(openStackContainer, s3Bucket):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_filename = f"{timestamp}_{openStackContainer}_to_{s3Bucket}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%a %Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def create_openstack_session():
    authUrl = os.getenv("OS_AUTH_URL")
    applicationCredentialId = os.getenv("OS_APPLICATION_CREDENTIAL_ID")
    applicationCredentialSecret = os.getenv("OS_APPLICATION_CREDENTIAL_SECRET")
    auth = auth_v3.ApplicationCredential(
        auth_url=authUrl,
        application_credential_id=applicationCredentialId,
        application_credential_secret=applicationCredentialSecret
    )
    return session.Session(auth=auth)

def create_s3_client(regionName):
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
        region_name=regionName
    )

def refresh_credentials(regionName):
    awsAccessKeyID = input("Enter AWS Access Key ID: ")
    awsSecretAccessKey = input("Enter AWS Secret Access Key: ")
    awsSessionToken = input("Enter AWS Session Token: ")
    os.environ["AWS_ACCESS_KEY_ID"] = awsAccessKeyID
    os.environ["AWS_SECRET_ACCESS_KEY"] = awsSecretAccessKey
    os.environ["AWS_SESSION_TOKEN"] = awsSessionToken
    return create_s3_client(regionName)

def create_temp_directory(tempDirectory):
    os.makedirs(tempDirectory, exist_ok=True)

def remove_temp_directory(tempDirectory, logger):
    if os.path.exists(tempDirectory):
        shutil.rmtree(tempDirectory)
        logger.info("Temporary files have been removed")

def ensure_bucket_exists(bucketName, s3Client, logger):
    try:
        s3Client.head_bucket(Bucket=bucketName)
    except ClientError as error:
        if error.response["Error"]["Code"] == "404":
            logger.error(f"Bucket {bucketName} does not exist. Exiting the script")
            raise Exception(f"S3 bucket '{bucketName}' does not exist.")
        else:
            logger.error(f"Failed to access bucket {bucketName}: {error}")
            raise

def list_openstack_objects(swiftConnection, containerName, logger):
    objects = []
    marker = None
    while True:
        page = swiftConnection.get_container(containerName, marker=marker)[1]
        if not page:
            break
        objects.extend(page)
        marker = page[-1]["name"]
    logger.debug(f"Retrieved {len(objects)} objects from container '{containerName}'.")
    return objects

def list_s3_objects(s3Client, bucketName, logger):
    objects = []
    continuationToken = None
    while True:
        if continuationToken:
            response = s3Client.list_objects_v2(Bucket=bucketName, ContinuationToken=continuationToken)
        else:
            response = s3Client.list_objects_v2(Bucket=bucketName)
        if 'Contents' in response:
            objects.extend(response['Contents'])
        if response.get('IsTruncated'):  # More data to fetch
            continuationToken = response.get('NextContinuationToken')
        else:
            break
    logger.debug(f"Retrieved {len(objects)} objects from S3 bucket '{bucketName}'.")
    return objects

def calculate_md5(filePath):
    hashMD5 = hashlib.md5()
    with open(filePath, "rb") as file:
        for chunk in iter(lambda: file.read(4096), b""):
            hashMD5.update(chunk)
    return hashMD5.hexdigest()

def upload_file_with_retry(localFilePath, bucketName, objectName, s3Client, config, retries, logger):
    attempt = 0
    while attempt < retries:
        try:
            s3Client.upload_file(localFilePath, bucketName, objectName, Config=config)
            logger.info(f"{objectName} uploaded successfully on attempt {attempt + 1}.")
            return True
        except ClientError as error:
            if error.response["Error"]["Code"] == "ExpiredToken":
                logger.warning("AWS session expired. Requesting new credentials.")
                s3Client = refresh_credentials(s3Client.meta.region_name)
                continue
            attempt += 1
            logger.error(f"Failed to upload {objectName} (attempt {attempt + 1}/{retries}): {error}")
            time.sleep(2 ** attempt)
    return False

def transfer_object(object, swiftConnection, s3Client, config, tempDirectory, openStackContainer, s3Bucket, retries, logger):
    objectName = object["name"]
    tempFilePath = os.path.join(tempDirectory, objectName)
    os.makedirs(os.path.dirname(tempFilePath), exist_ok=True)

    if objectName.endswith("/"):
        s3Client.put_object(Bucket=s3Bucket, Key=objectName)
        logger.info(f"Creating directory structure {objectName} in S3.")
        return

    _, objectContents = swiftConnection.get_object(openStackContainer, objectName)
    with open(tempFilePath, "wb") as file:
        file.write(objectContents)
    logger.debug(f"Downloaded {objectName} to {tempFilePath}")

    localMD5 = calculate_md5(tempFilePath)
    try:
        s3Head = s3Client.head_object(Bucket=s3Bucket, Key=objectName)
        s3MD5 = s3Head["ETag"].strip('"')
        if localMD5 == s3MD5:
            logger.info(f"{objectName} is up to date in S3. Skipping upload.")
            os.remove(tempFilePath)
            return
        else:
            logger.info(f"{objectName} exists but has changed. Overwriting {objectName}...")
    except ClientError as error:
        if error.response["Error"]["Code"] == "404":
            logger.info(f"{objectName} does not exist in S3. Uploading {objectName}...")
        else:
            logger.error(f"Failed to access object {objectName}: {error}")
            raise

    uploadSuccess = upload_file_with_retry(tempFilePath, s3Bucket, objectName, s3Client, config, retries, logger)
    if not uploadSuccess:
        logger.error(f"Failed to upload {objectName}. Exiting the script.")
    os.remove(tempFilePath)

def main(openStackContainer, s3Bucket, maxWorkers, regionName, bandwidthLimitMb):
    logger = create_logger(openStackContainer, s3Bucket)
    logger.info("Starting OpenStack to S3 transfer...")

    tempDirectory = f"/tmp/{s3Bucket}"

    create_temp_directory(tempDirectory)

    swiftConnection = swiftclient.Connection(session=create_openstack_session())
    s3Client = create_s3_client(regionName)
    ensure_bucket_exists(s3Bucket, s3Client, logger)

    config = TransferConfig(max_bandwidth=bandwidthLimitMb * 1024 * 1024)

    openstack_objects = list_openstack_objects(swiftConnection, openStackContainer, logger)

    retries = 3
    if not openstack_objects:
        logger.warning("No objects found in OpenStack container.")
        return

    with ThreadPoolExecutor(max_workers=maxWorkers) as executor:
        for obj in openstack_objects:
            executor.submit(transfer_object, obj, swiftConnection, s3Client, config, tempDirectory, openStackContainer, s3Bucket, retries, logger)
    remove_temp_directory(tempDirectory, logger)

    # Compare total objects between OpenStack and S3
    s3_objects = list_s3_objects(s3Client, s3Bucket, logger)
    logger.info(f"Total objects in OpenStack container '{openStackContainer}': {len(openstack_objects)}")
    logger.info(f"Total objects in S3 bucket '{s3Bucket}': {len(s3_objects)}")

    if len(openstack_objects) == len(s3_objects):
        logger.info("Object count matches between OpenStack container and S3 bucket.")
    else:
        logger.warning("Object count mismatch between OpenStack container and S3 bucket.")

    logger.info("OpenStack to S3 transfer process completed.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Transfer objects from OpenStack to AWS S3")
    parser.add_argument("--openStackContainer", type=str, required=True, help="Name of the OpenStack Container")
    parser.add_argument("--s3Bucket", type=str, required=True, help="Name of the S3 bucket")
    parser.add_argument("--maxWorkers", type=int, required=True, help="Number of workers for concurrent uploads (Minimum: 1)")
    parser.add_argument("--regionName", type=str, required=True, help="AWS Region name")
    parser.add_argument("--bandwidthLimitMb", type=int, required=True, help="Maximum bandwidth limit in MB for S3 uploads (Minimum: 1)")

    args = parser.parse_args()
    main(args.openStackContainer, args.s3Bucket, args.maxWorkers, args.regionName, args.bandwidthLimitMb)
