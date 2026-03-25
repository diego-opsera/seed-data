"""
loader.py — generate seed data and write it to Databricks.

Usage:
    # Preview what would be inserted (no writes):
    python loader.py --catalog opsera_prod --story baseline

    # Actually insert:
    python loader.py --catalog opsera_prod --story baseline --confirm

    # Delete all rows for the demo enterprise:
    python loader.py --catalog opsera_prod --delete --confirm

Environment variables required:
    DATABRICKS_HOST         e.g. https://adb-xxxx.azuredatabricks.net
    DATABRICKS_HTTP_PATH    e.g. /sql/1.0/warehouses/xxxx
    DATABRICKS_TOKEN        personal access token
"""
import argparse
import os
import sys
import yaml
from pathlib import Path
from datetime import date

from databricks import sql as dbsql

from generators import feature_level, ide_level, language_model_level, user_level, enterprise_level
from generators.user_level import build_user_row_dicts


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_yaml(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def load_config(story_name: str) -> tuple[dict, dict]:
    root = Path(__file__).parent
    entities = load_yaml(root / "config" / "entities.yaml")
    story = load_yaml(root / "config" / "stories" / f"{story_name}.yaml")
    return entities, story


# ---------------------------------------------------------------------------
# Databricks connection
# ---------------------------------------------------------------------------

def get_connection():
    host = os.environ.get("DATABRICKS_HOST")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH")
    token = os.environ.get("DATABRICKS_TOKEN")
    missing = [k for k, v in [
        ("DATABRICKS_HOST", host),
        ("DATABRICKS_HTTP_PATH", http_path),
        ("DATABRICKS_TOKEN", token),
    ] if not v]
    if missing:
        print(f"ERROR: Missing environment variables: {', '.join(missing)}")
        sys.exit(1)
    return dbsql.connect(server_hostname=host, http_path=http_path, access_token=token)


# ---------------------------------------------------------------------------
# Generation
# ---------------------------------------------------------------------------

def generate_all(catalog: str, entities: dict, story: dict) -> list[tuple[str, str]]:
    """Returns list of (table_name, sql_statement) pairs."""
    user_rows = build_user_row_dicts(entities, story)

    statements = []
    statements += [(feature_level.TABLE, s) for s in feature_level.generate(catalog, entities, story)]
    statements += [(ide_level.TABLE, s) for s in ide_level.generate(catalog, entities, story)]
    statements += [(language_model_level.TABLE, s) for s in language_model_level.generate(catalog, entities, story)]
    statements += [(user_level.TABLE, s) for s in user_level.generate(catalog, entities, story)]
    statements += [(enterprise_level.TABLE, s) for s in enterprise_level.generate(catalog, entities, story, user_rows)]
    return statements


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------

DELETE_TABLES = [
    "enterprise_level_copilot_metrics",
    "enterprise_user_level_copilot_metrics",
    "enterprise_user_feature_level_copilot_metrics",
    "enterprise_user_ide_level_copilot_metrics",
    "enterprise_user_language_model_level_copilot_metrics",
]

DEMO_ENTERPRISE_ID = 999999


def build_delete_statements(catalog: str) -> list[tuple[str, str]]:
    return [
        (t, f"DELETE FROM {catalog}.base_datasets.{t} WHERE enterprise_id = {DEMO_ENTERPRISE_ID}")
        for t in DELETE_TABLES
    ]


# ---------------------------------------------------------------------------
# Preview
# ---------------------------------------------------------------------------

def preview(statements: list[tuple[str, str]]):
    print("\n--- PREVIEW (no changes made) ---")
    for table, sql in statements:
        n = sql.count("\n  (")
        print(f"  {table}: {n} row(s)")
    print(f"\nTotal statements: {len(statements)}")
    print("Run with --confirm to execute, or --output-sql <dir> to write SQL files.\n")


# ---------------------------------------------------------------------------
# Output SQL to files
# ---------------------------------------------------------------------------

def output_sql(statements: list[tuple[str, str]], output_dir: str, story_name: str):
    import os
    os.makedirs(output_dir, exist_ok=True)
    written = []
    # Group by table so multiple statements for same table go in one file
    by_table: dict[str, list[str]] = {}
    for table, sql in statements:
        by_table.setdefault(table, []).append(sql)
    for table, sqls in by_table.items():
        filename = os.path.join(output_dir, f"{story_name}__{table}.sql")
        with open(filename, "w") as f:
            f.write("\n\n".join(sqls) + "\n")
        n = sum(s.count("\n  (") for s in sqls)
        written.append((filename, n))
    print(f"\nSQL files written to {output_dir}/:")
    for filename, n in written:
        print(f"  {os.path.basename(filename)}  ({n} rows)")
    print()


# ---------------------------------------------------------------------------
# Execute
# ---------------------------------------------------------------------------

def execute(statements: list[tuple[str, str]], conn):
    with conn.cursor() as cursor:
        for i, (table, sql) in enumerate(statements, 1):
            print(f"[{i}/{len(statements)}] {table}... ", end="", flush=True)
            cursor.execute(sql)
            print("done")
    print("\nAll statements executed successfully.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True, help="Databricks catalog name")
    parser.add_argument("--story", default="baseline", help="Story config name (without .yaml)")
    parser.add_argument("--confirm", action="store_true", help="Actually execute against Databricks")
    parser.add_argument("--output-sql", metavar="DIR", help="Write SQL files to DIR instead of executing")
    parser.add_argument("--delete", action="store_true", help="Delete demo data instead of inserting")
    args = parser.parse_args()

    if args.delete:
        statements = build_delete_statements(args.catalog)
        print(f"\nWill DELETE all rows WHERE enterprise_id = {DEMO_ENTERPRISE_ID} from:")
        for table, _ in statements:
            print(f"  {args.catalog}.base_datasets.{table}")
    else:
        entities, story = load_config(args.story)
        statements = generate_all(args.catalog, entities, story)
        preview(statements)

    # --output-sql: write to files, no DB connection needed
    if args.output_sql:
        output_sql(statements, args.output_sql, args.story)
        return

    if not args.confirm:
        if args.delete:
            print("\nRun with --confirm to execute deletes.\n")
        return

    conn = get_connection()
    try:
        execute(statements, conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
