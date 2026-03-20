from core.interfaces import BaseDBAdapter
from core.config.settings import get_settings


def create_db_adapter() -> BaseDBAdapter:
    """
    Returns the correct DB adapter based on DB_TYPE in .env.
    To add a new DB: implement BaseDBAdapter, add a case here.
    """
    db_type = get_settings().DB_TYPE.lower()

    if db_type == "mysql":
        from adapters.mysql_adapter import MySQLAdapter
        return MySQLAdapter()

    if db_type == "mongo":
        from adapters.mongo_adapter import MongoAdapter
        return MongoAdapter()

    raise ValueError(
        f"Unknown DB_TYPE='{db_type}'. "
        "Valid options: mysql | mongo"
    )
