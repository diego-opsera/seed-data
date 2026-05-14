# Confirm schema + current state of github_copilot_developer_usage_org_level —
# the table the Developer Language And Editor Usage dashboard actually reads
# from. Long-format / EAV-style: param_name in ('programmingLanguage', 'editor',
# 'ide_model', 'chat_model'), parameter holds the actual value.
#
# Run:
#   exec(open("/tmp/seed-data/notebooks/core/debug_dev_lang_editor_v3.py").read())

CATALOG = "playground_prod"
T = f"{CATALOG}.base_datasets.github_copilot_developer_usage_org_level"

print()
print("=" * 78)
print(f"Schema: {T}")
print("=" * 78)
try:
    spark.sql(f"DESCRIBE {T}").show(60, truncate=False)
except Exception as e:
    print(f"DESCRIBE failed: {str(e).splitlines()[0][:200]}")

print()
print("=" * 78)
print("Row count + org breakdown")
print("=" * 78)
try:
    spark.sql(f"""
      SELECT org_name, COUNT(*) AS rows,
             MIN(copilot_usage_date) AS min_date,
             MAX(copilot_usage_date) AS max_date
      FROM {T}
      GROUP BY org_name
      ORDER BY rows DESC
      LIMIT 20
    """).show(truncate=False)
except Exception as e:
    print(f"query failed: {str(e).splitlines()[0][:200]}")

print()
print("=" * 78)
print("Distinct param_name values (i.e., what categories this table tracks)")
print("=" * 78)
try:
    spark.sql(f"""
      SELECT param_name, COUNT(*) AS rows,
             COUNT(DISTINCT parameter) AS n_distinct_values
      FROM {T}
      GROUP BY param_name
      ORDER BY rows DESC
    """).show(truncate=False)
except Exception as e:
    print(f"query failed: {str(e).splitlines()[0][:200]}")

print()
print("=" * 78)
print("Top 10 parameter values per category (gives us the canonical names)")
print("=" * 78)
for cat in ['programmingLanguage', 'editor', 'ide_model', 'chat_model']:
    print(f"\n-- param_name = '{cat}' --")
    try:
        spark.sql(f"""
          SELECT parameter, COUNT(*) AS rows,
                 SUM(total_lines_suggested) AS total_sugg,
                 SUM(total_lines_accepted) AS total_acc
          FROM {T}
          WHERE param_name = '{cat}'
          GROUP BY parameter
          ORDER BY rows DESC
          LIMIT 10
        """).show(truncate=False)
    except Exception as e:
        print(f"  failed: {str(e).splitlines()[0][:200]}")

print()
print("=" * 78)
print("Sample row (so we know how each column is populated)")
print("=" * 78)
try:
    spark.sql(f"SELECT * FROM {T} LIMIT 3").show(truncate=False, vertical=True)
except Exception as e:
    print(f"failed: {str(e).splitlines()[0][:200]}")
