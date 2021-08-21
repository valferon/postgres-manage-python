""" Service class for postgresql manager.
S3 storage engine
"""
import boto3
from services.postgres import PostgreSQLManagerService


class PostgreSQLManagerServiceS3(PostgreSQLManagerService):
    """Main class for service."""

    aws_bucket_name: str
    aws_bucket_path: str

    def __init__(self, config_obj, verbose_mode):
        """Initialize instance.
        :param config_obj: configparser object from config file
        """
        super().__init__(self, config_obj, verbose_mode)
        self.aws_bucket_name = config_obj.get("S3", "bucket_name")
        self.aws_bucket_path = config_obj.get("S3", "bucket_backup_path")

    def _get_available_backups(self):
        """Return a list of available backups in S3 bucket."""
        s3_client = boto3.client("s3")
        s3_objects = s3_client.list_objects_v2(
            Bucket=self.aws_bucket_name, Prefix=self.aws_bucket_path
        )
        backup_list = [s3_content["Key"] for s3_content in s3_objects["Contents"]]
        return backup_list
