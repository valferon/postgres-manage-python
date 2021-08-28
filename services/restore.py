import gzip
import os
import shutil
import subprocess
from configparser import ConfigParser

import boto3
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from services import logger, tmp_dir
from services.list import ListStorageBackup


class Postgres:
    def __init__(self, config: ConfigParser):
        self.config = config
        self.kwargs = {
            'user': self.config.get('dest', 'user'),
            'password': self.config.get('dest', 'password'),
            'host': self.config.get('dest', 'host'),
            'port': self.config.get('dest', 'port'),
            'db': self.config.get('dest', 'db'),
        }
        self.kwargs['restore_db'] = f'{self.kwargs["db"]}_restore'

    def create(self):
        con = psycopg2.connect(
            dbname='postgres',
            port=self.kwargs['port'],
            user=self.kwargs['user'],
            host=self.kwargs['host'],
            password=self.kwargs['password'],
        )

        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        cur.execute(
            "SELECT pg_terminate_backend( pid ) "
            "FROM pg_stat_activity "
            "WHERE pid <> pg_backend_pid( ) "
            "AND datname = '{}'".format(self.kwargs['restore_db'])
        )
        cur.execute(f"DROP DATABASE IF EXISTS {self.kwargs['restore_db']}")
        cur.execute(f"CREATE DATABASE {self.kwargs['restore_db']}")
        cur.execute(
            f"GRANT ALL PRIVILEGES ON DATABASE {self.kwargs['restore_db']} TO {self.kwargs['user']}"
        )

    def restore(self, src):
        """Restore postgres db from a file."""
        # 'restore_executable' can take any arbitrary executable, even from inside of a docker container
        restore_executable = self.config.get('command', 'restore', fallback='pg_dump')
        command = restore_executable.split() + [
            "--no-owner",
            "--no-privileges",
            f'--dbname=postgresql://{self.kwargs["user"]}:{self.kwargs["password"]}@'
            f'{self.kwargs["host"]}:{self.kwargs["port"]}/{self.kwargs["restore_db"]}',
            src
        ]
        process = subprocess.Popen(command, subprocess.PIPE)

        output = process.communicate()[0]

        if int(process.returncode) != 0:
            raise Exception(f"Command failed. Output: code [{process.returncode}]: {output}")

        return output

    def switch(self):
        con = psycopg2.connect(
            dbname="postgres",
            port=self.kwargs['port'],
            user=self.kwargs['user'],
            host=self.kwargs['host'],
            password=self.kwargs['password'],
        )

        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        cur.execute(
            "SELECT pg_terminate_backend( pid ) "
            "FROM pg_stat_activity "
            "WHERE pid <> pg_backend_pid( ) "
            f"AND datname = '{self.kwargs['db']}'"
        )
        cur.execute(f"DROP DATABASE IF EXISTS {self.kwargs['db']}")
        cur.execute(
            f'ALTER DATABASE "{self.kwargs["restore_db"]}" RENAME TO "{self.kwargs["db"]}"'
        )

    def process(self, src):
        self.create()
        self.restore(src)
        self.switch()


class Restore:
    def __init__(self, config: ConfigParser, date: str):
        self.config = config
        # this is the date on which user had taken a backup
        self.date = date
        self.dest = os.path.join(tmp_dir, 'restore.dump.gz')

    @property
    def service(self):
        # dynamically return the relevant method
        return getattr(
            self,
            self.config.get('setup', 'engine', fallback='LOCAL').lower()
        )

    def get_backup(self):
        # for now we are only concerned with the latest version
        backup = next(
            filter(
                lambda _backup: self.date in _backup,
                ListStorageBackup(self.config).process(print_results=False)
            ),
            None
        )

        if not backup:
            raise Exception(f'No backup found for {self.date}, please choose another date!')

        return backup

    def s3(self):
        backup = self.get_backup()
        logger.info(f'Getting {backup} from S3 bucket')
        boto3.resource('s3').meta.client.download_file(
            self.config.get('S3', 'bucket_name'), backup, self.dest
        )
        logger.info(f'Saved the {backup} to {self.dest}')

    def local(self):
        backup = self.get_backup()
        logger.info(f'Loading {backup} to {self.dest}')
        backup_file = os.path.join(self.config.get('local_storage', 'path', fallback='./backups/'), backup)
        shutil.copy(backup_file, self.dest)

    def extract(self, src):
        dest, extension = os.path.splitext(src)

        with gzip.open(src, 'rb') as f_in, open(dest, 'wb') as f_out:
            f_out.writelines(f_in.readlines())

        return dest

    def process(self):
        if not self.date:
            raise Exception(
                'No date was chosen for restore. Run again with the "list" action to see available restore dates!'
            )

        self.service()
        extracted_file = self.extract(self.dest)
        Postgres(self.config).process(extracted_file)
