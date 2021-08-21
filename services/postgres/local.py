""" Service class for postgresql manager.
LOCAL storage engine
"""
import os
import shutil
from services.postgres import PostgreSQLManagerService


class PostgreSQLManagerServiceLocal(PostgreSQLManagerService):
    """Main class for service."""

    local_backup_path: str

    def __init__(self, config_obj, verbose_mode):
        """Initialize instance.
        :param config_obj: configparser object from config file
        """
        super().__init__(self, config_obj, verbose_mode)
        self.local_backup_path = config_obj.get(
            "local_storage", "path", fallback="./backups/"
        )

    def _get_available_backups(self):
        """Return a list of available backups in local storage."""
        backup_list = os.listdir(self.local_backup_path)
        return backup_list

    def store_backup(self, compressed_file):
        """Store DB backup locally."""
        backup_folder = self.local_backup_path
        try:
            check_folder = os.listdir(backup_folder)
        except FileNotFoundError:
            os.mkdir(backup_folder)
        shutil.move(
            compressed_file,
            "{}{}".format(self.local_backup_path, self.filename_compressed),
        )
