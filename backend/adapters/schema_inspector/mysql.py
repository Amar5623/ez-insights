from typing import Any


def inspect_mysql_schema(connection: Any) -> dict:
    """
    Extracts full schema from a live MySQL connection.

    Returns:
        {
            "table_name": [
                {"column": "id", "type": "int", "nullable": False},
                {"column": "name", "type": "varchar(255)", "nullable": True},
                ...
            ],
            ...
        }

    Dev 1 — implement this using SHOW TABLES + DESCRIBE <table>.
    """
    # TODO (Dev 1):
    # with connection.cursor() as cursor:
    #     cursor.execute("SHOW TABLES")
    #     tables = [row[f"Tables_in_{db}"] for row in cursor.fetchall()]
    #
    #     schema = {}
    #     for table in tables:
    #         cursor.execute(f"DESCRIBE `{table}`")
    #         rows = cursor.fetchall()
    #         schema[table] = [
    #             {
    #                 "column": row["Field"],
    #                 "type": row["Type"],
    #                 "nullable": row["Null"] == "YES",
    #             }
    #             for row in rows
    #         ]
    #     return schema
    raise NotImplementedError
