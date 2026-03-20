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
    if connection is None:
        raise RuntimeError("No connection provided to inspect_mysql_schema.")

    schema = {}

    with connection.cursor() as cursor:

        # Step 1 — get all table names
        # SHOW TABLES returns one row per table
        # DictCursor gives: {"Tables_in_nlsql_db": "books"}
        # we use list(row.values())[0] to grab the name
        # regardless of what the database is called
        cursor.execute("SHOW TABLES")
        tables = [list(row.values())[0] for row in cursor.fetchall()]

        # Step 2 — for each table get its columns
        for table in tables:
            cursor.execute(f"DESCRIBE `{table}`")
            rows = cursor.fetchall()

            # DESCRIBE returns one row per column like:
            # {"Field": "id", "Type": "int(11)", "Null": "NO", ...}
            schema[table] = [
                {
                    "column":   row["Field"],           # column name
                    "type":     row["Type"],             # e.g. int(11), varchar(255)
                    "nullable": row["Null"] == "YES",    # "YES" → True, "NO" → False
                }
                for row in rows
            ]

    return schema