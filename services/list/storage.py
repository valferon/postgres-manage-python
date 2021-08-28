import os
from configparser import ConfigParser

import boto3


class StorageBackup:
    def __init__(self, config: ConfigParser):
        self.config = config
        self.result = None

    def __str__(self):
        if not self.result:
            return None
        return '\n'.join(self.result)

    @property
    def service(self):
        # dynamically return the relevant method
        return getattr(
            self,
            self.config.get('setup', 'engine', fallback='LOCAL').lower()
        )

    def s3(self):
        s3_client = boto3.client('s3')
        s3_objects = s3_client.list_objects_v2(
            Bucket=self.config.get('S3', 'bucket_name'),
            Prefix=self.config.get('S3', 'bucket_backup_path'),
        )
        return [s3_content['Key'] for s3_content in s3_objects['Contents']]

    def local(self):
        backup_dir = self.config.get('local_storage', 'path', fallback='./backups/')
        try:
            return os.listdir(backup_dir)
        except FileNotFoundError:
            raise Exception(
                f'Could not found {backup_dir} when searching for backups.'
                f'Check your config file settings'
            )

    def process(self, print_results=True):
        self.result = sorted(self.service(), reverse=True)

        if print_results:
            print(self)
        return self.result
