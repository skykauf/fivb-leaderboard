#!/usr/bin/env bash
# Run the full test pipeline: ETL (with test limits) + dbt build.
# Uses env vars from env.test.example if present; otherwise uses defaults below.
set -e
cd "$(dirname "$0")/.."

if [ -f .venv/bin/activate ]; then
  set +e
  source .venv/bin/activate
  set -e
fi

if [ -f env.test.example ]; then
  set -a
  source env.test.example
  set +a
else
  export TRUNCATE_RAW=1
  export LIMIT_TOURNAMENTS=15
  export LIMIT_MATCHES_PER_TOURNAMENT=150
  export LIMIT_RESULTS_PER_TOURNAMENT=100
  export ETL_PARALLEL=0
fi

python -m etl.load_raw
dbt build
