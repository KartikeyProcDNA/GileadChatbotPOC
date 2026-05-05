import re
from dataclasses import dataclass, field
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from data_loader import DatabaseState

# ── Forbidden DML / DDL patterns (structural — never schema-dependent) ────────
_FORBIDDEN: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\bDROP\b",         re.IGNORECASE), "DROP statements are not permitted."),
    (re.compile(r"\bDELETE\b",       re.IGNORECASE), "DELETE statements are not permitted."),
    (re.compile(r"\bTRUNCATE\b",     re.IGNORECASE), "TRUNCATE statements are not permitted."),
    (re.compile(r"\bINSERT\b",       re.IGNORECASE), "INSERT statements are not permitted."),
    (re.compile(r"\bUPDATE\b",       re.IGNORECASE), "UPDATE statements are not permitted."),
    (re.compile(r"\bALTER\b",        re.IGNORECASE), "ALTER statements are not permitted."),
    (re.compile(r"\bCREATE\b",       re.IGNORECASE), "CREATE statements are not permitted."),
    (re.compile(r"\bEXEC(?:UTE)?\b", re.IGNORECASE), "EXEC/EXECUTE is not permitted."),
    (re.compile(r"--"),                               "SQL comments (--) are not permitted."),
    (re.compile(r";\s*\S", re.DOTALL),                "Multiple statements are not permitted."),
]

_TABLE_REF_PATTERN = re.compile(
    r"(?:FROM|JOIN)\s+([`\"\[]?[\w]+[`\"\]]?)",
    re.IGNORECASE,
)


def _extract_table_refs(sql: str) -> list[str]:
    """Return unique table names referenced after FROM / JOIN keywords."""
    refs: set[str] = set()
    for m in _TABLE_REF_PATTERN.finditer(sql):
        refs.add(re.sub(r'[`"\[\]]', "", m.group(1)))
    return list(refs)

@dataclass
class ValidationResult:
    valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    sanitized: str = ""

    def to_dict(self) -> dict:
        return {
            "valid": self.valid,
            "errors": self.errors,
            "warnings": self.warnings,
            "sanitized": self.sanitized,
        }

def validate_sql(
    sql: str | None,
    db_state: Optional["DatabaseState"] = None,
) -> ValidationResult:
    """
    Validate a SQL string against the currently loaded schema.

    Args:
        sql:      Raw SQL string from the LLM or user.
        db_state: Live DatabaseState from data_loader.get_db().
                  If None, table-name validation is skipped (no data loaded yet).

    Checks:
      1. Non-empty string
      2. Forbidden DML / DDL keywords
      3. Must start with SELECT
      4. Referenced table names exist in the loaded schema   (requires db_state)
      5. Warns on missing LIMIT
      6. Warns on unquoted column names that contain spaces  (derived from schema)
      7. Warns on SELECT *
      8. Never show the whole dataset (enforce advisory warnings, even if validation is disabled)
    """
    if not sql or not sql.strip():
        return ValidationResult(valid=False, errors=["No SQL provided."])

    trimmed = sql.strip()
    errors:   list[str] = []
    warnings: list[str] = []

    # 1. Forbidden patterns
    for pattern, message in _FORBIDDEN:
        if pattern.search(trimmed):
            errors.append(message)

    # 2. Must start with SELECT
    if not re.match(r"^\s*SELECT\b", trimmed, re.IGNORECASE):
        errors.append("Only SELECT queries are permitted.")

    # 3 & 6. Schema-dependent checks (only when data is loaded)
    if db_state is not None:
        known_tables = set(db_state.tables.keys())

        # 3. Unknown table names
        for tbl in _extract_table_refs(trimmed):
            if tbl not in known_tables:
                errors.append(
                    f'Unknown table: "{tbl}". '
                    f'Loaded tables: {", ".join(sorted(known_tables))}.'
                )

        # 6. Columns with spaces that need quoting — derived from actual schema
        all_space_cols: set[str] = set()
        for meta in db_state.tables.values():
            all_space_cols.update(meta.quoted_columns)

        for col in all_space_cols:
            # Match the column name not already surrounded by a quote character
            col_pat = re.compile(
                r'(?<!["\`])' + re.escape(col) + r'(?!["\`])',
                re.IGNORECASE,
            )
            if col_pat.search(trimmed):
                warnings.append(
                    f'Column "{col}" contains spaces — '
                    f'wrap it in double-quotes in your query.'
                )

    # 4. No LIMIT clause
    if not re.search(r"\bLIMIT\b", trimmed, re.IGNORECASE):
        warnings.append(
            "No LIMIT clause detected — query may return a large result set."
        )

    # 5. SELECT *
    if re.search(r"SELECT\s+\*", trimmed, re.IGNORECASE):
        warnings.append(
            "SELECT * returns all columns. "
            "Consider selecting only the columns you need."
        )

    sanitized = re.sub(r";\s*$", "", trimmed)

    return ValidationResult(
        valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        sanitized=sanitized,
    )
