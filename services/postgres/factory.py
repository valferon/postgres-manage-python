from services.postgres import (
    PostgreSQLManagerServiceS3,
    PostgreSQLManagerServiceLocal,
)


class PostgreSQLManagerServiceFactory:
    """Class for handling instantiation of service according to storage engine."""

    build_mapper = {
        "S3": PostgreSQLManagerServiceS3,
        "LOCAL": PostgreSQLManagerServiceLocal,
    }

    def __new__(cls, config_obj, verbose_mode):
        """Return corresponding instance of PostgreSQLManagerService."""
        storage_engine = config_obj.get("setup", "storage_engine")
        return cls.build_mapper(storage_engine)(config_obj, verbose_mode)
