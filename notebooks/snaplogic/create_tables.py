TARGET_CATALOG = "playground_prod"
TARGET_SCHEMA  = "source_to_stage"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")
print(f"Schema {TARGET_CATALOG}.{TARGET_SCHEMA} ready.")

_TBLPROPS = """
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors'   = 'true',
  'delta.feature.appendOnly'      = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants'      = 'supported',
  'delta.minReaderVersion'        = '3',
  'delta.minWriterVersion'        = '7')
"""

# ── raw_snaplogic_snaplex ────────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}.raw_snaplogic_snaplex (
  instance_id         STRING,
  label               STRING,
  environment         STRING,
  location            STRING,
  org                 STRING,
  cc_status           STRING,
  running_nodes_count INT,
  down_nodes_count    INT,
  max_slots           INT,
  max_mem             INT,
  reserved_slots      INT,
  time_created        TIMESTAMP,
  time_updated        TIMESTAMP
) {_TBLPROPS}
""")
print("Created raw_snaplogic_snaplex")

# ── raw_snaplogic_snaplex_nodes ──────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}.raw_snaplogic_snaplex_nodes (
  node_id              STRING,
  node_label           STRING,
  snaplex_name         STRING,
  snaplex_instance_id  STRING,
  environment          STRING,
  location             STRING,
  org                  STRING,
  node_status          STRING,
  cpu_cores            INT,
  total_memory_gb      DOUBLE,
  jvm_max_mem_gb       DOUBLE,
  total_swap_bytes     BIGINT,
  max_file_descriptors INT,
  create_time          TIMESTAMP
) {_TBLPROPS}
""")
print("Created raw_snaplogic_snaplex_nodes")

# ── raw_snaplogic_activities ─────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}.raw_snaplogic_activities (
  by_whom       STRING,
  org_label     STRING,
  environment   STRING,
  event_type    STRING,
  asset_label   STRING,
  project_label STRING,
  create_time   TIMESTAMP
) {_TBLPROPS}
""")
print("Created raw_snaplogic_activities")

# ── Verify ───────────────────────────────────────────────────────────────────
for tbl in ["raw_snaplogic_snaplex", "raw_snaplogic_snaplex_nodes", "raw_snaplogic_activities"]:
    n = spark.sql(f"SELECT COUNT(*) FROM {TARGET_CATALOG}.{TARGET_SCHEMA}.{tbl}").collect()[0][0]
    print(f"  {tbl}: {n} rows")
