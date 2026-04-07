# Insert a filter_groups_unity row so demo-acme-corp appears in the DORA hierarchy dropdown.
# This is a one-time configuration insert — it is NOT wiped by the weekly data refresh.
# Delete with delete_filter_group.py if needed.
# Run via exec(open("/tmp/seed-data/notebooks/dora/insert_filter_group.py").read())

import uuid

CATALOG = "playground_prod"

FGU_ID          = str(uuid.uuid4())
FILTER_GROUP_ID = str(uuid.uuid4())

spark.sql(f"""
    INSERT INTO {CATALOG}.master_data.filter_groups_unity
        (id, level_1, level_2, level_3, level_4, level_5,
         filter_group_id, createdBy, createdAt, updatedBy, updatedAt, active, roles)
    VALUES (
        '{FGU_ID}',
        'Acme Corp',
        '',
        'demo-acme-corp',
        '',
        '',
        '{FILTER_GROUP_ID}',
        'seed-data@demo.io',
        CURRENT_TIMESTAMP(),
        'seed-data@demo.io',
        CURRENT_TIMESTAMP(),
        true,
        null
    )
""")

print(f"id:              {FGU_ID}")
print(f"filter_group_id: {FILTER_GROUP_ID}")
print("level_1='Acme Corp', level_3='demo-acme-corp', active=true")
print("demo-acme-corp should now appear in the DORA hierarchy dropdown.")
