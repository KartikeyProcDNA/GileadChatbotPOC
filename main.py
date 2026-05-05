import logging
import tempfile
from pathlib import Path

from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from config import settings
from data_loader import load_file, get_db
from sql_agent import SQLAgent
from sql_validator import validate_sql

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title=settings.app_title,
    version=settings.app_version,
    description=(
        "Natural-language SQL query assistant. "
        "Upload any Excel or CSV file, then ask questions in plain English. "
        "Powered by Azure OpenAI and a live in-memory SQLite database."
    ),
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_agent = SQLAgent()


@app.on_event("startup")
async def startup_event():
    if settings.data_file_path:
        path = Path(settings.data_file_path)
        try:
            state = load_file(path)
            logger.info(
                "Auto-loaded '%s': %d tables",
                state.file_name,
                len(state.tables),
            )
        except Exception as exc:
            logger.error("Failed to auto-load DATA_FILE_PATH='%s': %s", path, exc)

class ChatMessage(BaseModel):
    role: str = Field(..., pattern="^(user|assistant)$")
    content: str = Field(..., min_length=1)


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1)
    history: list[ChatMessage] = Field(default_factory=list)


class ValidationRequest(BaseModel):
    sql: str = Field(..., min_length=1)


class ValidationResponse(BaseModel):
    valid: bool
    errors: list[str]
    warnings: list[str]
    sanitized: str


class ChatResponse(BaseModel):
    role: str
    content: str
    sql: str | None
    validation: dict | None
    results: list[dict] | None
    all_queries: list[dict] | None   # all investigation steps [{sql, results, validation, error}]
    error: str | None


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/", tags=["Health"])
async def root():
    """Health check. Shows whether a data file is loaded."""
    db = get_db()
    return {
        "status": "ok",
        "version": settings.app_version,
        "data_loaded": db is not None,
        "file_name": db.file_name if db else None,
        "table_count": len(db.tables) if db else 0,
        "loaded_at": db.loaded_at if db else None,
    }


@app.post("/data/upload", tags=["Data"])
async def upload_file(file: UploadFile = File(...)):
    """
    Upload an Excel (.xlsx / .xls) or CSV file to load into the query engine.

    The file is saved to a temporary location, parsed, and loaded into an
    in-memory SQLite database. Previous data is replaced.
    """
    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in {".xlsx", ".xls", ".csv"}:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type '{suffix}'. Upload .xlsx, .xls, or .csv.",
        )

    # Write to a named temp file so pandas/openpyxl can seek it properly
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = Path(tmp.name)

    try:
        db = load_file(tmp_path)
        # Rename the internal file_name to the original upload name
        db.file_name = file.filename or tmp_path.name
    except Exception as exc:
        tmp_path.unlink(missing_ok=True)
        raise HTTPException(status_code=422, detail=str(exc))
    finally:
        tmp_path.unlink(missing_ok=True)

    return {
        "message": f"File '{db.file_name}' loaded successfully.",
        "file_name": db.file_name,
        "loaded_at": db.loaded_at,
        "tables": {name: meta.row_count for name, meta in db.tables.items()},
    }


@app.get("/data/info", tags=["Data"])
async def data_info():
    """Return full schema info for the currently loaded file."""
    db = get_db()
    if db is None:
        raise HTTPException(status_code=404, detail="No data file loaded. POST to /data/upload first.")
    return db.to_info_dict()


@app.get("/data/tables", tags=["Data"])
async def list_tables():
    """List all loaded tables with row counts and column counts."""
    db = get_db()
    if db is None:
        raise HTTPException(status_code=404, detail="No data file loaded.")
    return [
        {
            "name": name,
            "original_name": meta.original_name,
            "row_count": meta.row_count,
            "column_count": len(meta.columns),
        }
        for name, meta in db.tables.items()
    ]


@app.get("/data/tables/{table_name}", tags=["Data"])
async def get_table(table_name: str):
    """Full column details for a specific table."""
    db = get_db()
    if db is None:
        raise HTTPException(status_code=404, detail="No data file loaded.")
    meta = db.tables.get(table_name)
    if meta is None:
        raise HTTPException(
            status_code=404,
            detail=f'Table "{table_name}" not found. '
                   f'Available: {", ".join(db.tables)}',
        )
    return meta.to_dict()


@app.post("/chat", response_model=ChatResponse, tags=["Chat"])
async def chat(request: ChatRequest):
    """
    Send a natural-language question; receive SQL, validation result, and query results.
    A data file must be loaded before using this endpoint.
    """
    db = get_db()
    history = [{"role": m.role, "content": m.content} for m in request.history]
    response = await _agent.run(request.message, history, db_state=db)
    return response.to_dict()


@app.post("/validate", response_model=ValidationResponse, tags=["SQL"])
async def validate(request: ValidationRequest):
    """
    Validate a SQL string against the loaded schema without executing it.
    Table-name checking is only performed when a file is loaded.
    """
    db = get_db()
    result = validate_sql(request.sql, db_state=db)
    return result.to_dict()


@app.get("/relationships", tags=["Data"])
async def get_relationships():
    """
    Return inferred column-overlap relationships between loaded tables.
    Two tables are related if they share a column name — no hardcoded join keys.
    """
    db = get_db()
    if db is None:
        raise HTTPException(status_code=404, detail="No data file loaded.")

    # Build column → [tables] index
    col_index: dict[str, list[str]] = {}
    for tname, tmeta in db.tables.items():
        for col in tmeta.column_names:
            col_index.setdefault(col, []).append(tname)

    # Relationships: columns shared by 2+ tables
    relationships = []
    for col, tables in col_index.items():
        if len(tables) >= 2:
            for i in range(len(tables)):
                for j in range(i + 1, len(tables)):
                    relationships.append({
                        "column": col,
                        "table_a": tables[i],
                        "table_b": tables[j],
                        "join_hint": f"{tables[i]}.{col} = {tables[j]}.{col}",
                    })

    return {"relationships": relationships, "total": len(relationships)}
