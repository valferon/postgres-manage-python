import subprocess
from configparser import ConfigParser
from functools import cached_property

from services import logger


class PostgresBackup:
    def __init__(self, config: ConfigParser):
        self.config = config
        self.result = None

    def __str__(self):
        if not self.result:
            return None

        return '\n'.join(self.result.splitlines())

    @cached_property
    def pg_kwargs(self):
        return {
            'user': self.config.get('postgresql', 'user'),
            'password': self.config.get('postgresql', 'password'),
            'host': self.config.get('postgresql', 'host'),
            'port': self.config.get('postgresql', 'port'),
            'db': self.config.get('postgresql', 'db')
        }

    def process(self, print_results=True):
        process = subprocess.Popen(
            [
                'psql',
                f"--dbname=postgresql://{self.pg_kwargs['user']}:{self.pg_kwargs['password']}@"
                f"{self.pg_kwargs['host']}:{self.pg_kwargs['port']}/{self.pg_kwargs['db']}",
                '--list',
            ],
            stdout=subprocess.PIPE,
        )
        output = process.communicate()[0]

        if int(process.returncode) != 0:
            logger.error(f'Command failed with return code {process.returncode}')
            raise Exception('Non-zero return code!')

        self.result = output

        if print_results:
            print(self)

        return self.result
