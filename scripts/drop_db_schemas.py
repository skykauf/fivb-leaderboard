#!/usr/bin/env python3
"""Drop all project schemas (raw, staging, core, mart) from the Postgres database."""
import sys

# Allow running from repo root
sys.path.insert(0, ".")

from etl.db import drop_all_schemas, get_engine

if __name__ == "__main__":
    engine = get_engine()
    drop_all_schemas(engine)
    print("Dropped all schemas (raw, staging, core, mart). You can run ETL and dbt anew.")
