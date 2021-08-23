import gzip
import os
import shutil
import subprocess

import boto3
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from services import logger
from services.list import ListStorageBackup


class Postgres:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def create(self):
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
            "AND datname = '{}'".format(self.kwargs['restore_db'])
        )
        cur.execute(f"DROP DATABASE IF EXISTS {self.kwargs['restore_db']}")
        cur.execute(f"CREATE DATABASE {self.kwargs['restore_db']}")
        cur.execute(
            f"GRANT ALL PRIVILEGES ON DATABASE {self.kwargs['restore_db']} TO {self.kwargs['user']}"
        )
        return self.kwargs['restore_db']

    def restore(self, src):
        """Restore postgres db from a file."""
        # 'restore_executable' can take any arbitrary executable, even from inside of a docker container
        command = self.kwargs['restore_executable'].split() + [
            "--no-owner",
            f'--dbname=postgresql://{self.kwargs["user"]}:{self.kwargs["password"]}@'
            f'{self.kwargs["host"]}:{self.kwargs["port"]}/{self.kwargs["restore_db"]}',
            src
        ]
        process = subprocess.Popen(command, subprocess.PIPE)

        output = process.communicate()[0]

        if int(process.returncode) != 0:
            raise Exception(f"Command failed. Output: code [{process.returncode}]: {output}", )

        return output

    def switch(self, _from, _to):
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
            f"AND datname = '{_to}'"
        )
        cur.execute(f"DROP DATABASE IF EXISTS {_to}")
        cur.execute(
            f'ALTER DATABASE "{_from}" RENAME TO "{_to}"'
        )

    def process(self, src):
        self.create()
        self.restore(src)
        self.switch(self.kwargs['restore_db'], self.kwargs['db'])


class Restore:
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
        return [
            self.get_backup(self.kwargs['date']),
            self.kwargs['dest']
        ]

    def get_backup(self, date):
        # for now we are only concerned with the latest version
        backup = next(
            filter(
                lambda _backup: date in _backup,
                ListStorageBackup(self.engine, self.config).process(print_results=False)
            ),
            None
        )

        if not backup:
            raise Exception(f'No backup found for {date}, please choose another date!')

        return backup

    def s3(self, backup, dest):
        logger.info(f'Getting {backup} from S3 bucket')
        boto3.resource('s3').meta.client.download_file(
            self.config['AWS_BUCKET_NAME'], backup, dest
        )
        logger.info(f'Saved the {backup} to {dest}')

    def local(self, backup, dest):
        logger.info(f'Loading {backup} to {dest}')
        shutil.copy(
            f"{self.config['LOCAL_BACKUP_PATH']}/{backup}",
            dest
        )

    def extract(self, src):
        dest, extension = os.path.splitext(src)

        with gzip.open(src, "rb") as f_in, open(dest, "wb") as f_out:
            f_out.writelines(f_in.readlines())

        return dest

    def process(self):
        if not self.kwargs.get('date'):
            raise Exception(
                'No date was chosen for restore. Run again with the "list" action to see available restore dates!'
            )

        self.service(*self.args)
        self.kwargs['dest'] = self.extract(self.kwargs['dest'])
        Postgres(**self.kwargs).process(self.kwargs['dest'])
