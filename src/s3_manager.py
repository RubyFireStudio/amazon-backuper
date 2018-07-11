from io import StringIO
import math
import os
import logging

import boto3

from botocore.exceptions import ClientError

from .settings import BACKUP_DIR
from .utils import get_md5, ProgressPercentage

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class S3Manager:
    BUCKET_NAME = 'S3_BUCKET_NAME'

    def __init__(self, bucket_name):
        self.BUCKET_NAME = bucket_name
        self.client = boto3.client('s3')

    def list_files(self, headers={}):
        resp = self.client.list_objects_v2(
            Bucket=self.BUCKET_NAME,
            Metadata=headers
        )
        return resp.get('Contents', [])

    def upload_files(self, file_list):
        count = 0
        for file in file_list:
            try:
                self.upload_file(file, '/')
            except ClientError:
                logger.error('Error in uploading {} file'.format(file.name))
            else:
                count += 1
        return count

    def upload_file(self, full_filename, dest='', headers={}):
        return self.client.upload_file(
            full_filename,
            self.BUCKET_NAME,
            dest + os.path.basename(full_filename),
            Callback=ProgressPercentage(full_filename)
        )

    def delete_file(self, file_key, source):
        if file_key:
            self.client.delete_object(
                Bucket=self.BUCKET_NAME,
                Key=source + file_key
            )

    def delete_directory(self, prefix):
        object_list = self.client.list_objects(
            Bucket=self.BUCKET_NAME,
            Prefix=prefix
        )['Contents']

        for object in object_list:
            self.client.delete_object(
                Bucket=self.BUCKET_NAME,
                Key=object['Key']
            )

    def update_file(self, file_key_to_del, file_to_update, update_path, headers=None):
        self.delete_file(file_key_to_del, update_path)
        return self.upload_file(file_to_update, update_path, headers=headers)
