# Diagnostic round 9: find real working DF data to understand what the chart actually queries
# Run via exec(open("/tmp/seed-data/notebooks/dora/diag_dora_9.py").read())

import json

CATALOG  = "playground_prod"
DF_KPI   = "60aed2f8-1c74-4792-ad51-bf4e5a65f7b9"
OUR_FGID = "6b35e559-cde7-4c79-b3c2-891589f706fa"

def sql(q): return spark.sql(q)
def rows(q, n=5): return [r.asDict() for r in sql(q).limit(n).collect()]
def out(label, data): print(f"\n### {label}"); print(json.dumps(data, default=str, indent=2))

# ── 1. Real filter_values_unity rows for DF KPI (not ours) ────────────────────
out("fvu.real_df_kpi_rows", rows(f"""
    SELECT filter_group_id, tool_type, filter_name, filter_values, active
    FROM {CATALOG}.master_data.filter_values_unity
    WHERE array_contains(kpi_uuids, '{DF_KPI}')
      AND filter_group_id != '{OUR_FGID}'
""", 5))

# ── 2. View output for a real org with DF data ─────────────────────────────────
# Get a real level_3 that has DF configured
real_df_fgids = [r["filter_group_id"] for r in sql(f"""
    SELECT DISTINCT filter_group_id
    FROM {CATALOG}.master_data.filter_values_unity
    WHERE array_contains(kpi_uuids, '{DF_KPI}')
      AND filter_group_id != '{OUR_FGID}'
""").limit(3).collect()]
out("real_df_fgids", real_df_fgids)

if real_df_fgids:
    fgid = real_df_fgids[0]
    # Get the level_3 for this filter_group_id
    level_rows = sql(f"""
        SELECT level_3, level_1 FROM {CATALOG}.master_data.filter_groups_unity
        WHERE filter_group_id = '{fgid}'
    """).collect()
    out("real_df_level", [r.asDict() for r in level_rows])

    if level_rows:
        lvl3 = level_rows[0]["level_3"]

        # What does the view return for this level_3?
        out("view.real_df_row", rows(f"""
            SELECT level_3, filter_group_id, kpi_uuids, tool_type, filter_name, filter_values
            FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
            WHERE level_3 = '{lvl3}' AND kpi_uuids = '{DF_KPI}'
        """, 3))

        # Simulate the join — what does it return?
        out("real_df_join_sample", rows(f"""
            SELECT pa.pipeline_source, pa.project_url, pa.pipeline_id, pa.pipeline_name,
                   pa.step_type, pa.pipeline_status, pa.pipeline_started_at,
                   pa.data_source, pa.tool_identifier
            FROM {CATALOG}.base_datasets.pipeline_activities pa
            JOIN (
                SELECT DISTINCT exploded_project_url AS project_url
                FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
                LATERAL VIEW EXPLODE(project_url) AS exploded_project_url
                WHERE level_3 = '{lvl3}' AND kpi_uuids = '{DF_KPI}'
            ) f ON pa.project_url = f.project_url
            WHERE pa.pipeline_status IN ('success', 'failed')
        """, 3))

# ── 3. Check if DF might join on pipeline_id instead of project_url ────────────
# Look at what filter_name values exist for DF KPI rows
out("fvu.df_kpi_filter_names", [r.asDict() for r in sql(f"""
    SELECT filter_name, tool_type, COUNT(*) AS n
    FROM {CATALOG}.master_data.filter_values_unity
    WHERE array_contains(kpi_uuids, '{DF_KPI}')
    GROUP BY 1, 2
    ORDER BY n DESC
""").limit(10).collect()])
