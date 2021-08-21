""" Service class for postgresql manager. """
import datetime
import gzip
import subprocess


class PostgreSQLManagerService:
    """Main class for service."""

    def __init__(self, config_obj, verbose_mode):
        """Initialize instance.
        :param config_obj: configparser object from config file
        """
        self.postgres_host = config_obj.get("postgresql", "host")
        self.postgres_port = config_obj.get("postgresql", "port")
        self.postgres_db = config_obj.get("postgresql", "db")
        self.postgres_restore = "{}_restore".format(self.postgres_db)
        self.postgres_user = config_obj.get("postgresql", "user")
        self.postgres_password = config_obj.get("postgresql", "password")
        self.timestr = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        self.filename = "backup-{}-{}.dump".format(self.timestr, self.postgres_db)
        self.filename_compressed = "{}.gz".format(self.filename)
        self.temporary_path = config_obj.get(
            "setup", "temporary_path", fallback="/tmp/"
        )
        self.restore_filename = f"{self.temporary_path}restore.dump.gz"
        self.restore_uncompressed = f"{self.temporary_path}restore.dump"
        self.temporary_file_path = "{}{}".format(self.temporary_path, self.filename)
        self.verbose_mode = verbose_mode

    def list_available_backups(self):
        """Return a list of available backups with descending order."""
        backup_list = sorted(self._get_available_backups(), reverse=True)
        return backup_list

    def list_available_databases(self):
        """Return a list of databases on a postgresql server."""
        try:
            process = subprocess.Popen(
                [
                    "psql",
                    "--dbname=postgresql://{}:{}@{}:{}/{}".format(
                        self.postgres_user,
                        self.postgres_password,
                        self.postgres_host,
                        self.postgres_port,
                        self.postgres_db,
                    ),
                    "--list",
                ],
                stdout=subprocess.PIPE,
            )
            output = process.communicate()[0]
            if int(process.returncode) != 0:
                print("Command failed. Return code : {}".format(process.returncode))
            return output
        except Exception as e:
            print(e)

    def create_backup(self):
        """Create DB backup and store it according to storage engine.
        :return: tuple(Bool, List) with success status and dump process result."""
        dump_result = self._generate_db_dump()
        if dump_result:
            compressed_file = self._compress_file(self.temporary_file_path)
            self.store_backup(compressed_file)
            return True, dump_result
        return False, []

    def restore_backup(self):
        """Restore a DB backup."""
        pass

    def _get_available_backups(self):
        """Return a list of available backups."""
        pass

    def _generate_db_dump(self):
        """Create DB dump returning process response."""
        try:
            subprocess_params = [
                "pg_dump",
                "--dbname=postgresql://{}:{}@{}:{}/{}".format(
                    self.postgres_user,
                    self.postgres_password,
                    self.postgres_host,
                    self.postgres_port,
                    self.postgres_db,
                ),
                "-Fc",
                "-f",
                self.temporary_file_path,
            ]

            if self.verbose_mode:
                subprocess_params.append("-v")

            process = subprocess.Popen(subprocess_params, stdout=subprocess.PIPE)
            output = process.communicate()[0]
            if int(process.returncode) != 0:
                print("Command failed. Return code : {}".format(process.returncode))
            return output
        except Exception as e:
            print(e)

    def _compress_file(self, src_file):
        compressed_file = "{}.gz".format(str(src_file))
        with open(src_file, "rb") as f_in:
            with gzip.open(compressed_file, "wb") as f_out:
                for line in f_in:
                    f_out.write(line)
        return compressed_file
