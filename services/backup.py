import gzip
import os
import shutil
import subprocess
from configparser import ConfigParser
from datetime import datetime

import boto3

from services import logger, tmp_dir


class BackupPostgres:
    def __init__(self, config: ConfigParser):
        self.config = config

    def backup_postgres_db(self):
        user = self.config.get('src', 'user')
        password = self.config.get('src', 'password')
        host = self.config.get('src', 'host')
        port = self.config.get('src', 'port')
        db = self.config.get('src', 'db')

        file = os.path.join(tmp_dir, db)
        try:
            # 'dump_executable' can take any arbitrary executable, even from inside of a docker container
            dump_executable = self.config.get('command', 'dump', fallback='pg_dump')
            command = dump_executable.split() + [
                '-Fc',
                '--no-owner',
                f'--dbname=postgresql://{user}:{password}@{host}:{port}/{db}',
                '-f',
                file,
            ]
            process = subprocess.Popen(command, stdout=subprocess.PIPE)
            output = process.communicate()[0]
            if process.returncode != 0:
                # remove the file which was created
                self.clean_up(file)
                raise Exception(output)
            return file
        except Exception as e:
            logger.error(e)
            raise e

    def compress_file(self, src, compressed):
        print(src, compressed)
        with open(src, 'rb') as f_in, gzip.open(compressed, 'wb') as f_out:
            f_out.writelines(f_in.readlines())

    def clean_up(self, file):
        try:
            os.remove(file)
        except PermissionError:
            logger.error(f'Could not remove the temp file {file}!')

    def backup(self, compressed_file):
        sql_file_path = self.backup_postgres_db()
        self.compress_file(sql_file_path, compressed_file)
        self.clean_up(sql_file_path)


class Backup:
    def __init__(self, config: ConfigParser):
        self.config = config

        time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.filename = f'backup-{time_str}-{self.config.get("src", "db")}.dump'
        self.file_path = os.path.join(tmp_dir, self.filename)
        self.compressed_file = f'{self.filename}.gz'

    @property
    def service(self):
        # dynamically return the relevant method
        return getattr(
            self,
            self.config.get('setup', 'engine', fallback='LOCAL').lower()
        )

    def s3(self):
        """
        Upload a file to an AWS S3 bucket.
        """
        boto3.client('s3').upload_file(
            self.file_path,
            self.config.get('S3', 'bucket_name'),
            f'{self.config.get("S3", "bucket_backup_path")}{self.compressed_file}',
        )
        os.remove(self.file_path)

    def local(self):
        # Move compressed backup into {LOCAL_BACKUP_PATH}
        backup_dir = self.config.get('local_storage', 'path', fallback='./backups/')
        os.makedirs(backup_dir, exist_ok=True)
        dest = os.path.join(backup_dir, self.compressed_file)

        print(self.compressed_file, dest)
        shutil.move(self.compressed_file, dest)

    def process(self):
        BackupPostgres(self.config).backup(self.compressed_file)
        self.service()
