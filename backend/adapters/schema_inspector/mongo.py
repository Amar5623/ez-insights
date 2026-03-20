from typing import Any
from bson import ObjectId


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
    if db is None:
        raise RuntimeError("No database provided to inspect_mongo_schema.")

    schema = {}

    # Step 1 — get all collection names
    for name in db.list_collection_names():

        # Step 2 — sample first 100 documents from this collection
        docs = list(db[name].find().limit(sample_size))

        # Step 3 — loop ALL docs and collect unique fields + infer types
        fields = {}   # field_name → inferred_type_string

        for doc in docs:
            for key, value in doc.items():
                if key not in fields:   # first time seeing this field
                    if isinstance(value, ObjectId):
                        fields[key] = "ObjectId"          # handle specially
                    else:
                        fields[key] = type(value).__name__ # e.g. str, float, int, list

        # Step 4 — shape into required list of dicts
        schema[name] = [
            {"field": k, "inferred_type": v}
            for k, v in fields.items()
        ]

    return schema