import boto3
import os
import logging

from botocore.exceptions import ClientError

from .settings import BACKUP_BUCKET_NAME, BACKUP_DIR, BACKUP_CHECK_HASH
from .utils import get_md5


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

s3 = boto3.client('s3')


def get_all_backup_files():
    return os.listdir(BACKUP_DIR)


def get_files_to_backup():
    backuped_objects = s3.list_objects_v2(Bucket=BACKUP_BUCKET_NAME).get('Contents', [])
    files_to_backup = get_all_backup_files()
    logger.debug('Files in S3: ' + ', '.join([obj['Key'] for obj in backuped_objects]))
    
    if not BACKUP_CHECK_HASH:
        # Check only for name
        backuped_files = [obj['Key'] for obj in backuped_objects]

        not_backuped_files = [f for f in files_to_backup if f not in backuped_files]
    else:
        # Check name and hash sum
        backuped_files = {obj['Key']: obj['ETag'].strip('"').strip("'") for obj in backuped_objects}
        not_backuped_files = []
        for filename in files_to_backup:
            full_filename = os.path.join(BACKUP_DIR, filename)
            md5 = get_md5(full_filename)
            if backuped_files.get(filename) != md5:
                not_backuped_files.append(filename)
        
    logger.debug('Files to upload: ' + ', '.join(not_backuped_files))
    return not_backuped_files


def upload_files(file_list):
    count = 0
    for filename in file_list:
        file = os.path.join(BACKUP_DIR, filename)
        try:
            s3.upload_file(file, BACKUP_BUCKET_NAME, filename)
        except ClientError:
            logger.error('Error in uploading {} file'.format(file))
        else:
            count += 1
    if len(file_list) > 0:
        logger.info('Upload {}/{} files'.format(count, len(file_list)))
    else:
        logger.info('No new files to upload')


def backup():
    files_to_upload = get_files_to_backup()
    upload_files(files_to_upload)
