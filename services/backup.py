import gzip
import os
import shutil
import subprocess

import boto3

from services import logger


class BackupPostgres:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def backup_postgres_db(self, file):
        """
        Backup postgres db to a file.
        """
        try:
            process = subprocess.Popen(
                [
                    'pg_dump',
                    f'--dbname=postgresql://{self.kwargs["user"]}:{self.kwargs["password"]}@'
                    f'{self.kwargs["host"]}:{self.kwargs["port"]}/{self.kwargs["db"]}',
                    '-f',
                    file,
                ],
                stdout=subprocess.PIPE,
            )
            output = process.communicate()[0]
            if process.returncode != 0:
                logger('Command failed. Return code : {}'.format(process.returncode))
                raise Exception(output)
            return output
        except Exception as e:
            logger.error(e)
            raise e

    def compress_file(self, src):
        compressed = f'{src}.gz'
        with open(src, 'rb') as f_in, gzip.open(compressed, 'wb') as f_out:
            f_out.writelines(f_in.readlines())
        return compressed

    def backup(self, src):
        self.backup_postgres_db(src)
        return self.compress_file(src)


class Backup:
    def __init__(self, engine: str, config, **kwargs):
        self.engine = engine.lower()
        self.config = config
        self.kwargs = kwargs

    @property
    def service(self):
        services = {
            's3': self.s3,
            'local': self.local,
        }
        return services[self.engine]

    @property
    def args(self):
        args = {
            's3': [
                self.kwargs['sql_file'],
                self.kwargs['compressed_file']
            ],
            'local': [
                self.kwargs['sql_file'],
                self.kwargs['compressed_file']
            ]
        }
        return args[self.engine]

    @property
    def db_kwargs(self):
        return {
            'user': self.kwargs['user'],
            'password': self.kwargs['password'],
            'host': self.kwargs['host'],
            'port': self.kwargs['port'],
            'db': self.kwargs['db'],
        }

    def s3(self, src, location):
        """
        Upload a file to an AWS S3 bucket.
        """
        boto3.client('s3').upload_file(
            src,
            self.config['AWS_BUCKET_NAME'],
            f'{self.config["AWS_BUCKET_PATH"]}{location}',
        )
        os.remove(src)

    def local(self, src, compressed):
        """Move compressed backup into {LOCAL_BACKUP_PATH}."""
        backup_dir = self.config['LOCAL_BACKUP_PATH']
        os.makedirs(backup_dir, exist_ok=True)
        shutil.move(
            src,
            f'{self.config["LOCAL_BACKUP_PATH"]}{compressed}'
        )

    def process(self):
        self.kwargs['compressed_file'] = BackupPostgres(**self.db_kwargs).backup(self.kwargs['sql_file'])
        self.service(*self.args)
