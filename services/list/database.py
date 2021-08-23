import subprocess

from services import logger


class PostgresBackup:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.result = None

    def __str__(self):
        if not self.result:
            return None

        return '\n'.join(self.result.splitlines())

    def process(self, print_results=True):
        process = subprocess.Popen(
            [
                "psql",
                f"--dbname=postgresql://{self.kwargs['user']}:{self.kwargs['password']}@"
                f"{self.kwargs['host']}:{self.kwargs['port']}/{self.kwargs['db']}",
                "--list",
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
