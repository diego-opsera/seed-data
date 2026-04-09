# Diagnostic: verify devex filter group is wired up and data is reachable
# Run via: exec(open("/tmp/seed-data/notebooks/devex/diag_devex_2.py").read())

import json

CATALOG = "playground_prod"
ORG     = "demo-acme-direct"

def sql(q):
    return spark.sql(q)

def rows(q, limit=20):
    return [r.asDict() for r in sql(q).limit(limit).collect()]

def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))

FGU  = f"{CATALOG}.master_data.filter_groups_unity"
FVU  = f"{CATALOG}.master_data.filter_values_unity"
FGVF = f"{CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity"
PR   = f"{CATALOG}.base_datasets.pull_requests"
CM   = f"{CATALOG}.base_datasets.commits_rest_api"

# ── 1. What filter groups exist for seed-data@devex.io? ─────────────────────
out("filter_groups_unity.devex_rows", rows(f"""
    SELECT id, filter_group_id, level_1, level_3, createdBy, createdAt
    FROM {FGU} WHERE createdBy = 'seed-data@devex.io'
"""))

# ── 2. What filter values exist for seed-data@devex.io? ─────────────────────
out("filter_values_unity.devex_rows", rows(f"""
    SELECT filter_group_id, tool_type, filter_name, filter_values,
           SIZE(kpi_uuids) AS kpi_count, created_by
    FROM {FVU} WHERE created_by = 'seed-data@devex.io'
"""))

# ── 3. Is the devex filter group visible in the flattened view? ──────────────
out("fgvf.devex_rows", rows(f"""
    SELECT level_1, level_3, id,
           project_url, project_name, team_names, board_ids,
           issue_status, include_issue_types, created_by
    FROM {FGVF}
    WHERE created_by = 'seed-data@devex.io'
"""))

# ── 4. Simulate the whereClause for the devex filter group ──────────────────
# The dashboard passes whereClause = "WHERE fgvf.id = '{FGU_ID}'"
# Get the FGU_ID for the devex group
fgu_id_rows = sql(f"SELECT id FROM {FGU} WHERE createdBy = 'seed-data@devex.io'").collect()
if fgu_id_rows:
    FGU_ID = fgu_id_rows[0]["id"]
    out("filter_group.FGU_ID", FGU_ID)

    out("fgvf.with_where_clause", rows(f"""
        SELECT project_url, project_name, team_names, board_ids,
               issue_status, include_issue_types
        FROM {FGVF}
        WHERE id = '{FGU_ID}'
    """))

    # Does the whereClause + commit join return rows?
    out("commits.matching_filter", rows(f"""
        WITH filter AS (
            SELECT DISTINCT exploded_project_url AS project_url
            FROM {FGVF}
            LATERAL VIEW explode_outer(project_url) AS exploded_project_url
            WHERE id = '{FGU_ID}'
            AND project_url IS NOT NULL
        )
        SELECT c.project_url, COUNT(*) AS n
        FROM {CM} c
        INNER JOIN filter f ON c.project_url = f.project_url
        WHERE c.org_name = '{ORG}'
        GROUP BY c.project_url
    """))

    out("prs.matching_filter", rows(f"""
        WITH filter AS (
            SELECT DISTINCT exploded_project_url AS project_url
            FROM {FGVF}
            LATERAL VIEW explode_outer(project_url) AS exploded_project_url
            WHERE id = '{FGU_ID}'
            AND project_url IS NOT NULL
        )
        SELECT pr.project_url, COUNT(*) AS n
        FROM {PR} pr
        INNER JOIN filter f ON pr.project_url = f.project_url
        WHERE pr.org_name = '{ORG}'
        GROUP BY pr.project_url
    """))
else:
    out("filter_groups_unity.devex_rows", "NO ROWS FOUND — devex filter group was not inserted")
