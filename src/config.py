from jsonschema import validate

schema = {
    "type": "object",
    "properties": {
        "deploy_dir": {"type": "string"},
        "template_dir": {"type": "string"},
        "storage_path": {"type": "string"},
        "notebooks": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "type": {"type": "string", "enum": ["sql", "py"]},
                    "desc": {"type": "string"},
                    "dlt": {"type": "boolean"},
                    "data_sources": {
                        "type": "array",
                        "items": {"type": "string"},
                        "minItems": 1,
                    },
                    "gold_agg_buckets": {
                        "type": "array",
                        "items": {"type": "string"},
                        "minItems": 1,
                    },
                },
                "required": ["id", "type"],
            },
            "minItems": 1,
            "uniqueItems": True,
        },
    },
    "required": ["deploy_dir", "template_dir", "storage_path", "notebooks"],
}

def validate_config(cfg):
    return validate(instance=cfg, schema=schema)


if __name__ == "__main__":
    cfg = {
        "deploy_dir": "deploy",
        "template_dir": "templates",
        "storage_path": "/tmp/lipyeow_ctx",
        "tgt_db_name": "lipyeow_ctx",
        "hello": "world",
        "notebooks": [
            {
                "type": "py",
                "desc": "sample collector to poll okta data into bronze table",
                "id": "okta_collector",
            }
        ],
    }

    validate_config(cfg)
