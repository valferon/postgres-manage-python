import os
from typing import Dict

import boto3


class StorageBackup:
    def __init__(self, engine: str, config):
        self.engine = engine
        self.config = config
        self.result = None

    def __str__(self):
        if not self.result:
            return None
        return '\n'.join(self.result)

    @property
    def service(self):
        services = {
            's3': self.s3,
            'local': self.local
        }
        return services[self.engine.lower()]

    def s3(self):
        # logger.info('Listing S3 bucket s3://{}/{} content :'.format(aws_bucket_name, aws_bucket_path))
        s3_client = boto3.client("s3")
        s3_objects = s3_client.list_objects_v2(
            Bucket=self.config['AWS_BUCKET_NAME'],
            Prefix=self.config['AWS_BUCKET_PATH'],
        )
        return [s3_content["Key"] for s3_content in s3_objects["Contents"]]

    def local(self):
        backup_dir = self.config['LOCAL_BACKUP_PATH']
        try:
            return os.listdir(backup_dir)
        except FileNotFoundError as e:
            print(
                f"Could not found {backup_dir} when searching for backups."
                f"Check your .config file settings"
            )
            raise e

    def process(self):
        self.result = sorted(self.service(), reverse=True)
        return self.result
