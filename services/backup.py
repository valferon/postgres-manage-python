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
        try:
            # 'dump_executable' can take any arbitrary executable, even from inside of a docker container
            command = self.kwargs['dump_executable'].split() + [
                '-Fc',
                '--no-owner',
                f'--dbname=postgresql://{self.kwargs["user"]}:{self.kwargs["password"]}@'
                f'{self.kwargs["host"]}:{self.kwargs["port"]}/{self.kwargs["db"]}',
                '-f',
                file,
            ]
            process = subprocess.Popen(command, stdout=subprocess.PIPE, )
            output = process.communicate()[0]
            if process.returncode != 0:
                # remove the file which was created
                os.remove(file)
                raise Exception(output)
            return output
        except Exception as e:
            logger.error(e)
            raise e

    def compress_file(self, src, compressed):
        with open(src, 'rb') as f_in, gzip.open(compressed, 'wb') as f_out:
            f_out.writelines(f_in.readlines())

    def clean_up(self, file):
        os.remove(file)

    def backup(self, sql_file, compressed_file):
        self.backup_postgres_db(sql_file)
        self.compress_file(sql_file, compressed_file)
        self.clean_up(sql_file)


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
                self.kwargs['compressed_file']
            ]
        }
        return args[self.engine]

    @property
    def db_kwargs(self):
        return {
            'dump_executable': self.kwargs['dump_executable'],
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

    def local(self, compressed):
        """Move compressed backup into {LOCAL_BACKUP_PATH}."""
        backup_dir = self.config['LOCAL_BACKUP_PATH']
        os.makedirs(backup_dir, exist_ok=True)
        shutil.move(
            compressed,
            f'{self.config["LOCAL_BACKUP_PATH"]}{self.kwargs["compressed_file"]}'
        )

    def process(self):
        BackupPostgres(**self.db_kwargs).backup(self.kwargs['sql_file'], self.kwargs['compressed_file'])
        self.service(*self.args)
