"""
Static lookup: master_data.file_extensions.
Maps file extensions to programming language names.
Required by copilot_contribution_report.sql for the language_written CTE,
which joins commits_rest_api file_extension values to language names.

This is a reference table — insert-once, no date-range dependency.
"""

TABLE  = "file_extensions"
SCHEMA = "master_data"

MERGE_SQL = """\
MERGE INTO {catalog}.master_data.file_extensions AS target
USING (
  SELECT col1 AS code_file_extension, col2 AS code_language
  FROM VALUES {values} AS t(col1, col2)
) AS source
ON target.code_file_extension = source.code_file_extension
WHEN NOT MATCHED THEN INSERT (code_file_extension, code_language)
  VALUES (source.code_file_extension, source.code_language);"""

# Covers the five languages in entities.yaml plus common others.
# Extensions must match the values generated in commits.py file_extension arrays.
_MAPPINGS = [
    ("ts",    "typescript"),
    ("tsx",   "typescript"),
    ("py",    "python"),
    ("go",    "go"),
    ("cs",    "csharp"),
    ("ex",    "elixir"),
    ("exs",   "elixir"),
    ("js",    "javascript"),
    ("jsx",   "javascript"),
    ("java",  "java"),
    ("rb",    "ruby"),
    ("rs",    "rust"),
    ("cpp",   "cpp"),
    ("kt",    "kotlin"),
    ("swift", "swift"),
    ("php",   "php"),
    ("scala", "scala"),
    ("sh",    "shell"),
    ("vue",   "vue"),
]


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    value_lines = [f"  ('{ext}', '{lang}')" for ext, lang in _MAPPINGS]
    return [MERGE_SQL.format(catalog=catalog, values=",\n".join(value_lines))]
