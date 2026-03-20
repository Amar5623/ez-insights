from typing import Any


def inspect_mongo_schema(db: Any, sample_size: int = 100) -> dict:
    """
    Infers schema from a live MongoDB database by sampling documents.

    Returns:
        {
            "collection_name": [
                {"field": "price", "inferred_type": "float"},
                {"field": "name",  "inferred_type": "str"},
                ...
            ],
            ...
        }

    Dev 1 — implement this by sampling top `sample_size` docs per collection.
    """
    # TODO (Dev 1):
    # schema = {}
    # for name in db.list_collection_names():
    #     docs = list(db[name].find().limit(sample_size))
    #     fields = {}
    #     for doc in docs:
    #         for key, value in doc.items():
    #             if key not in fields:
    #                 fields[key] = type(value).__name__
    #     schema[name] = [{"field": k, "inferred_type": v} for k, v in fields.items()]
    # return schema
    raise NotImplementedError
