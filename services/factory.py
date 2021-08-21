from services.backup import Backup
from services.list import ListStorageBackup, ListPostgresBackup
from services.restore import Restore


class ArgsFactory:
    @staticmethod
    def get_service(action, **kwargs):
        services = {
            'list': ListStorageBackup,
            'list_dbs': ListPostgresBackup,
            'backup': Backup,
            'restore': Restore,
        }

        try:
            return services[action](**kwargs)
        except KeyError:
            raise NotImplementedError(f'The action {action} is not implemented yet!')
