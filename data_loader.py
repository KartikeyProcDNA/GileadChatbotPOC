import re
import sqlite3
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

@dataclass
class ColumnMeta:
    name: str
    dtype: str
    has_spaces: bool


@dataclass
class TableMeta:
    name: str          # SQL-safe identifier
    original_name: str # sheet / filename
    columns: list[ColumnMeta]
    row_count: int

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def quoted_columns(self) -> list[str]:
        """Column names that require double-quoting in SQL (contain spaces)."""
        return [c.name for c in self.columns if c.has_spaces]

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "original_name": self.original_name,
            "columns": self.column_names,
            "column_details": [
                {"name": c.name, "dtype": c.dtype, "has_spaces": c.has_spaces}
                for c in self.columns
            ],
            "row_count": self.row_count,
            "quoted_columns": self.quoted_columns,
        }


@dataclass
class DatabaseState:
    con: sqlite3.Connection
    tables: dict[str, TableMeta]
    file_name: str
    file_path: str
    loaded_at: str

    def to_info_dict(self) -> dict:
        """Serialisable summary — does NOT include the connection object."""
        return {
            "file_name": self.file_name,
            "file_path": self.file_path,
            "loaded_at": self.loaded_at,
            "table_count": len(self.tables),
            "tables": {name: meta.to_dict() for name, meta in self.tables.items()},
        }

_db_state: Optional[DatabaseState] = None


def get_db() -> Optional[DatabaseState]:
    """Return the currently loaded DatabaseState, or None if no file loaded yet."""
    return _db_state

def _safe_table_name(raw: str) -> str:
    """
    Convert an arbitrary sheet/file name into a valid SQLite identifier.
    Replaces all non-alphanumeric characters with underscores;
    prefixes with 't_' if the name starts with a digit.
    """
    safe = re.sub(r"[^\w]", "_", raw.strip())
    if safe and safe[0].isdigit():
        safe = "t_" + safe
    return safe

def load_file(path: str | Path) -> DatabaseState:
    """
    Load an Excel (.xlsx, .xls) or CSV file into an in-memory SQLite database.

    Each sheet in Excel becomes a table.
    A single CSV file becomes one table named after the filename stem.

    Returns a DatabaseState and also stores it as the module-level singleton.
    Raises ValueError for unsupported formats; re-raises IO errors as-is.
    """
    global _db_state

    path = Path(path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Data file not found: {path}")

    suffix = path.suffix.lower()
    if suffix not in {".xlsx", ".xls", ".csv"}:
        raise ValueError(
            f"Unsupported file format '{suffix}'. "
            "Please provide an .xlsx, .xls, or .csv file."
        )

    logger.info("Loading data file: %s", path)

    if suffix in {".xlsx", ".xls"}:
        sheet_map: dict[str, pd.DataFrame] = pd.read_excel(
            path, sheet_name=None, dtype=str   # read everything as str first
        )
    else:  # CSV
        df = pd.read_csv(path, dtype=str)
        sheet_map = {path.stem: df}

    if not sheet_map:
        raise ValueError("The file contains no sheets / data.")


    con = sqlite3.connect(":memory:", check_same_thread=False)
    tables: dict[str, TableMeta] = {}

    for sheet_name, df in sheet_map.items():
        if df.empty:
            logger.warning("Skipping empty sheet: %s", sheet_name)
            continue

    
        df.columns = [str(c).strip() for c in df.columns]

    
        for col in df.columns:
            try:
                coerced = pd.to_numeric(df[col], errors="coerce")
                # Only apply if at least 80% of non-null values converted cleanly
                non_null = df[col].notna().sum()
                converted = coerced.notna().sum()
                if non_null > 0 and converted / non_null >= 0.8:
                    df[col] = coerced
            except Exception:
                pass

        table_name = _safe_table_name(sheet_name)

        # Write to SQLite
        df.to_sql(table_name, con, if_exists="replace", index=False)

        # Build column metadata
        col_metas = [
            ColumnMeta(
                name=col,
                dtype=str(df[col].dtype),
                has_spaces=" " in col,
            )
            for col in df.columns
        ]

        tables[table_name] = TableMeta(
            name=table_name,
            original_name=sheet_name,
            columns=col_metas,
            row_count=len(df),
        )
        logger.info("  Loaded table '%s' (%d rows, %d cols)", table_name, len(df), len(df.columns))

    if not tables:
        raise ValueError("No usable data found in the file (all sheets were empty).")

    _db_state = DatabaseState(
        con=con,
        tables=tables,
        file_name=path.name,
        file_path=str(path),
        loaded_at=datetime.now(timezone.utc).isoformat(),
    )

    logger.info("Database ready: %d tables loaded from '%s'", len(tables), path.name)
    return _db_state


def execute_query(sql: str, limit: int = 500) -> list[dict]:
    """
    Execute a validated SELECT query against the loaded in-memory SQLite database.

    Args:
        sql:   A sanitised SQL string (no trailing semicolon).
        limit: Hard cap on returned rows (protects against accidental full scans).

    Returns:
        List of row dicts with column names as keys.

    Raises:
        RuntimeError: If no data file has been loaded yet.
        sqlite3.Error: On any SQL execution error.
    """
    db = get_db()
    if db is None:
        raise RuntimeError(
            "No data file loaded. POST a file to /data/upload first."
        )

    # Enforce a hard row limit — append or replace LIMIT clause
    sql_stripped = sql.rstrip().rstrip(";")
    if not re.search(r"\bLIMIT\b", sql_stripped, re.IGNORECASE):
        sql_stripped = f"{sql_stripped} LIMIT {limit}"

    cursor = db.con.execute(sql_stripped)
    col_names = [d[0] for d in cursor.description]
    rows = [dict(zip(col_names, row)) for row in cursor.fetchall()]
    return rows
