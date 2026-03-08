#!/usr/bin/env python3
"""
Summary statistics for all columns in raw schema tables.

Computes per column: row count, null count, null %, distinct count, and for
numeric/date columns min/max. Use after running the pipeline to inspect data quality.

Usage:
  python scripts/raw_column_stats.py              # print table to stdout
  python scripts/raw_column_stats.py --csv        # CSV to stdout
  python scripts/raw_column_stats.py --json       # JSON to stdout
  python scripts/raw_column_stats.py --update-staging-schema  # enrich staging schema.yml

Requires DATABASE_URL in .env (same as ETL/dbt).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Project root
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text

from etl.db import get_engine

RAW_SCHEMA = "raw"

# Types we compute min/max for
NUMERIC_OR_DATE_TYPES = {
    "integer", "bigint", "smallint", "numeric", "real", "double precision",
    "date", "timestamp without time zone", "timestamp with time zone", "time",
}


def get_raw_tables_and_columns(engine):
    """Return list of (table_name, column_name, data_type) for schema raw."""
    q = text("""
        SELECT table_name, column_name, data_type, ordinal_position
        FROM information_schema.columns
        WHERE table_schema = :schema
        AND table_name LIKE 'raw_%'
        ORDER BY table_name, ordinal_position
    """)
    with engine.connect() as conn:
        rows = conn.execute(q, {"schema": RAW_SCHEMA}).fetchall()
    return [(r[0], r[1], r[2], r[3]) for r in rows]


def get_table_stats(engine, table_name: str, columns: list[tuple[str, str]]) -> list[dict]:
    """Compute row count, null count, null %, distinct count, and optional min/max per column."""
    # Build one aggregate query: count(*), and per column count(col), count(distinct col), [min, max]
    selects = ["count(*) AS _row_count"]
    col_names = []
    for col_name, data_type in columns:
        col_names.append((col_name, data_type))
        safe = f'"{col_name}"'
        selects.append(f"count({safe}) AS _n_{col_name}")
        selects.append(f"count(distinct {safe}) AS _d_{col_name}")
        if data_type in NUMERIC_OR_DATE_TYPES:
            selects.append(f"min({safe}) AS _min_{col_name}")
            selects.append(f"max({safe}) AS _max_{col_name}")
    sql = f'SELECT {", ".join(selects)} FROM "{RAW_SCHEMA}"."{table_name}"'
    with engine.connect() as conn:
        row = conn.execute(text(sql)).fetchone()
    if not row:
        return []
    keys = list(row._mapping.keys())
    row_count = row._mapping["_row_count"] or 0
    results = []
    idx = 1
    for col_name, data_type in col_names:
        n_key = f"_n_{col_name}"
        d_key = f"_d_{col_name}"
        non_null = row._mapping.get(n_key)
        distinct = row._mapping.get(d_key)
        null_count = (row_count - non_null) if (row_count is not None and non_null is not None) else None
        null_proportion = (null_count / row_count) if (row_count and row_count > 0 and null_count is not None) else None
        min_val = row._mapping.get(f"_min_{col_name}") if f"_min_{col_name}" in row._mapping else None
        max_val = row._mapping.get(f"_max_{col_name}") if f"_max_{col_name}" in row._mapping else None
        # Coerce for JSON (e.g. Decimal, date)
        if min_val is not None and hasattr(min_val, "isoformat"):
            min_val = min_val.isoformat()
        if max_val is not None and hasattr(max_val, "isoformat"):
            max_val = max_val.isoformat()
        results.append({
            "table": table_name,
            "column": col_name,
            "data_type": data_type,
            "row_count": row_count,
            "null_count": null_count,
            "null_proportion": round(null_proportion, 4) if null_proportion is not None else None,
            "distinct_count": distinct,
            "min": min_val,
            "max": max_val,
        })
        idx += 3
        if data_type in NUMERIC_OR_DATE_TYPES:
            idx += 2
    return results


def get_raw_column_stats(engine) -> list[dict]:
    """Compute stats for every column in every raw table."""
    table_cols = get_raw_tables_and_columns(engine)
    by_table = {}
    for table_name, column_name, data_type, _ in table_cols:
        by_table.setdefault(table_name, []).append((column_name, data_type))
    all_stats = []
    for table_name in sorted(by_table.keys()):
        cols = by_table[table_name]
        try:
            all_stats.extend(get_table_stats(engine, table_name, cols))
        except Exception as e:
            all_stats.append({
                "table": table_name,
                "column": None,
                "error": str(e),
                "row_count": None,
                "null_count": None,
                "null_proportion": None,
                "distinct_count": None,
                "min": None,
                "max": None,
            })
    return all_stats


def _format_pct(null_proportion):
    if null_proportion is None:
        return "—"
    return f"{100 * null_proportion:.1f}%"


def print_table(stats: list[dict]) -> None:
    """Print stats as a plain text table."""
    if not stats:
        print("No raw tables or columns found.")
        return
    # Filter to rows that have column info (skip error rows for display)
    rows = [s for s in stats if s.get("column")]
    if not rows:
        print("No column stats.")
        return
    col_widths = {
        "table": max(len(str(r.get("table", ""))) for r in rows) + 1,
        "column": max(len(str(r.get("column", ""))) for r in rows) + 1,
        "data_type": 24,
        "row_count": 10,
        "null_proportion": 10,
        "distinct": 10,
        "min": 14,
        "max": 14,
    }
    col_widths["table"] = max(col_widths["table"], 22)
    col_widths["column"] = max(col_widths["column"], 18)
    tw, cw = col_widths["table"], col_widths["column"]
    fmt = f"{{table:<{tw}}} {{column:<{cw}}} {{data_type:<24}} {{row_count:>10}} {{null_proportion:>10}} {{distinct:>10}} {{min:>14}} {{max:>14}}"
    header = fmt.format(table="table", column="column", data_type="data_type", row_count="row_count", null_proportion="null_%", distinct="distinct", min="min", max="max")
    print(header)
    print("-" * len(header))
    for r in rows:
        min_s = str(r["min"])[:14] if r.get("min") is not None else "—"
        max_s = str(r["max"])[:14] if r.get("max") is not None else "—"
        print(
            fmt.format(
                table=(r["table"] or "")[: col_widths["table"] - 1],
                column=(r["column"] or "")[: col_widths["column"] - 1],
                data_type=(r.get("data_type") or "")[:24],
                row_count=r.get("row_count") or "—",
                null_proportion=_format_pct(r.get("null_proportion")),
                distinct=r.get("distinct_count") if r.get("distinct_count") is not None else "—",
                min=min_s,
                max=max_s,
            )
        )


def raw_table_to_staging_model(raw_table: str) -> str | None:
    """Map raw table name to staging model name."""
    if not raw_table.startswith("raw_fivb_"):
        return None
    return "stg_fivb_" + raw_table.replace("raw_fivb_", "", 1)


# Staging column name -> raw column name when different
STAGING_TO_RAW_COLUMN = {"round_code": "round"}


def update_staging_schema(stats: list[dict], schema_path: Path) -> None:
    """
    Update staging schema.yml: append raw stats to column descriptions where the
    staging model and column match a raw table/column.
    """
    import re
    stats_by_key = {}
    for s in stats:
        if not s.get("column"):
            continue
        key = (s["table"], s["column"])
        stats_by_key[key] = s
    raw_to_stg = {t: raw_table_to_staging_model(t) for t in {s["table"] for s in stats if s.get("table")}}
    text = schema_path.read_text()
    lines = text.split("\n")
    out = []
    i = 0
    current_model = None
    while i < len(lines):
        line = lines[i]
        out.append(line)
        model_match = re.match(r"^\s+-\s+name:\s+(stg_fivb_\w+)\s*$", line)
        if model_match:
            current_model = model_match.group(1)
        col_match = re.match(r"^(\s+)-\s+name:\s+(\w+)\s*$", line)
        if col_match and current_model:
            indent, col_name = col_match.group(1), col_match.group(2)
            raw_col = STAGING_TO_RAW_COLUMN.get(col_name, col_name)
            raw_table = None
            for raw, stg in raw_to_stg.items():
                if stg == current_model:
                    raw_table = raw
                    break
            if raw_table and (raw_table, raw_col) in stats_by_key:
                s = stats_by_key[(raw_table, raw_col)]
                null_proportion = s["null_proportion"]
                distinct = s["distinct_count"]
                suffix = f" Raw: null {100 * null_proportion:.1f}%, {distinct} distinct." if null_proportion is not None and distinct is not None else ""
                if suffix:
                    if i + 1 < len(lines) and re.match(r"\s+description:\s+", lines[i + 1]):
                        i += 1
                        desc_line = lines[i]
                        if suffix.rstrip() not in desc_line:
                            desc_line = desc_line.rstrip()
                            if desc_line.endswith('"'):
                                desc_line = desc_line[:-1] + suffix + '"'
                            else:
                                desc_line = desc_line + suffix
                            out.append(desc_line)
                    else:
                        out.append(f'{indent}  description: "{col_name}{suffix}"')
        i += 1
    schema_path.write_text("\n".join(out) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Raw schema column statistics")
    parser.add_argument("--json", action="store_true", help="Output JSON to stdout")
    parser.add_argument("--csv", action="store_true", help="Output CSV to stdout")
    parser.add_argument("--update-staging-schema", action="store_true", help="Update staging schema.yml with raw stats in descriptions")
    args = parser.parse_args()
    engine = get_engine()
    stats = get_raw_column_stats(engine)
    if args.update_staging_schema:
        schema_path = ROOT / "models" / "staging" / "fivb" / "schema.yml"
        if not schema_path.exists():
            print("Staging schema not found:", schema_path, file=sys.stderr)
            sys.exit(1)
        update_staging_schema(stats, schema_path)
        print("Updated", schema_path, file=sys.stderr)
        return
    if args.json:
        # Filter to serializable rows; remove error placeholder rows or serialize
        out = []
        for s in stats:
            if s.get("column") is None and "error" in s:
                out.append({"table": s["table"], "error": s["error"]})
            else:
                out.append({k: v for k, v in s.items() if k != "column" or v is not None})
        print(json.dumps(out, indent=2))
    elif args.csv:
        import csv
        writer = csv.DictWriter(sys.stdout, fieldnames=["table", "column", "data_type", "row_count", "null_count", "null_proportion", "distinct_count", "min", "max"], extrasaction="ignore")
        writer.writeheader()
        for s in stats:
            if s.get("column") is not None:
                writer.writerow(s)
    else:
        print_table(stats)


if __name__ == "__main__":
    main()
