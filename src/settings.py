import os
import logging
from pathlib import Path
from dotenv import load_dotenv


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)

load_dotenv(dotenv_path=os.path.join(ROOT_DIR, '.env'))

logging.basicConfig(format='%(levelname)s:%(asctime)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.WARNING)


BACKUP_BUCKET_NAME = os.environ.get('BACKUP_BUCKET_NAME')

BACKUP_DIR = os.environ.get('BACKUP_DIR')

BACKUP_CHECK_HASH = os.environ.get('BACKUP_CHECK_HASH', default=False)
