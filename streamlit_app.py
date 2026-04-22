import requests
import pandas as pd
import streamlit as st

try:
    from config import settings
    API_BASE = settings.fastapi_base_url.rstrip("/")
except Exception:
    API_BASE = "http://localhost:8000"


st.set_page_config(
    page_title="Data Query Assistant",
    page_icon="⚕",
    layout="wide",
    initial_sidebar_state="expanded",
)


st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&display=swap');

  .stApp { background-color: #060b14; color: #f1f5f9; }

  section[data-testid="stSidebar"] {
    background-color: #080e1c;
    border-right: 1px solid #1e293b;
  }
  section[data-testid="stSidebar"] * { color: #94a3b8 !important; }
  section[data-testid="stSidebar"] h1,
  section[data-testid="stSidebar"] h2,
  section[data-testid="stSidebar"] h3 { color: #38bdf8 !important; }

  .user-msg {
    background: linear-gradient(135deg, #0ea5e9, #6366f1);
    border-radius: 18px 18px 4px 18px;
    padding: 12px 16px; margin: 8px 0 8px 60px;
    color: #fff; font-size: 0.9rem; line-height: 1.6;
  }
  .assistant-msg {
    background: #111827; border: 1px solid #1e293b;
    border-radius: 4px 18px 18px 18px;
    padding: 12px 16px; margin: 8px 60px 8px 0;
    color: #e2e8f0; font-size: 0.9rem; line-height: 1.6;
  }
  .msg-label {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.62rem; color: #334155; margin-bottom: 3px;
    letter-spacing: 0.07em;
  }
  .sql-block {
    background: #060b14; border: 1px solid #1e3a5f;
    border-radius: 8px; padding: 12px 14px; margin-top: 6px;
    font-family: 'IBM Plex Mono', monospace; font-size: 0.78rem;
    color: #7dd3fc; white-space: pre-wrap; overflow-x: auto;
  }
  .valid-badge {
    background: #0a1f0f; border: 1px solid #166534;
    border-radius: 6px; padding: 6px 10px; margin-top: 6px;
    font-family: 'IBM Plex Mono', monospace; font-size: 0.7rem; color: #86efac;
  }
  .invalid-badge {
    background: #1c0a0a; border: 1px solid #7f1d1d;
    border-radius: 6px; padding: 6px 10px; margin-top: 6px;
    font-family: 'IBM Plex Mono', monospace; font-size: 0.7rem; color: #fca5a5;
  }
  .warn-line { color: #fde68a; }
  .error-block {
    background: #1c0505; border: 1px solid #7f1d1d;
    border-radius: 8px; padding: 8px 12px; margin-top: 6px;
    font-family: 'IBM Plex Mono', monospace; font-size: 0.75rem; color: #fca5a5;
  }
  .rel-row {
    font-family: 'IBM Plex Mono', monospace; font-size: 0.72rem;
    padding: 5px 0; color: #94a3b8; border-bottom: 1px solid #1e293b;
  }

  div[data-testid="stButton"] > button {
    background: #0a0f1a !important; border: 1px solid #1e293b !important;
    color: #64748b !important; border-radius: 20px !important;
    font-size: 0.72rem !important; padding: 4px 12px !important; margin: 2px !important;
  }
  div[data-testid="stButton"] > button:hover {
    border-color: #38bdf8 !important; color: #38bdf8 !important;
  }
  textarea, .stTextInput input {
    background: #0a0f1a !important; color: #f1f5f9 !important;
    border: 1px solid #334155 !important; border-radius: 10px !important;
  }
  hr { border-color: #1e293b; }
</style>
""", unsafe_allow_html=True)


# ── API helpers ───────────────────────────────────────────────────────────────
def api_get(path: str, timeout: int = 5) -> dict | list | None:
    try:
        r = requests.get(f"{API_BASE}{path}", timeout=timeout)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.ConnectionError:
        return None
    except Exception:
        return None


def api_post(path: str, payload: dict, timeout: int = 180) -> dict | None:
    try:
        r = requests.post(f"{API_BASE}{path}", json=payload, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.ConnectionError:
        return {
            "role": "assistant",
            "content": f"⚠️ Cannot connect to API at `{API_BASE}`. Is FastAPI running?",
            "sql": None, "validation": None, "results": None, "all_queries": None,
            "error": "ConnectionError",
        }
    except Exception as exc:
        return {
            "role": "assistant",
            "content": f"⚠️ An unexpected error occurred: {exc}",
            "sql": None, "validation": None, "results": None, "all_queries": None,
            "error": str(exc),
        }


def upload_file(file_bytes: bytes, filename: str) -> dict | None:
    try:
        r = requests.post(
            f"{API_BASE}/data/upload",
            files={"file": (filename, file_bytes)},
            timeout=60,
        )
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        return {"error": str(exc)}


# ── Cached schema fetchers ────────────────────────────────────────────────────
@st.cache_data(ttl=30)
def fetch_schema() -> dict | None:
    return api_get("/data/info")


@st.cache_data(ttl=30)
def fetch_relationships() -> list:
    resp = api_get("/relationships")
    if isinstance(resp, dict):
        return resp.get("relationships", [])
    return []


def invalidate_schema_cache():
    fetch_schema.clear()
    fetch_relationships.clear()


# ── Session state init ────────────────────────────────────────────────────────
if "messages" not in st.session_state:
    st.session_state.messages = [
        {
            "role": "assistant",
            "content": (
                "👋 **Welcome to the Data Query Assistant.**\n\n"
                "Upload a data file using the sidebar to get started. "
                "I will introspect its schema automatically — "
                "no configuration needed.\n\n"
                "Once a file is loaded, ask me anything about your data in plain English."
            ),
            "sql": None, "validation": None, "results": None, "error": None,
        }
    ]

if "pending_query" not in st.session_state:
    st.session_state.pending_query = ""

if "data_loaded" not in st.session_state:
    st.session_state.data_loaded = False


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚕ Data Query Assistant")
    st.markdown(
        "<div style='font-family:IBM Plex Mono,monospace;font-size:0.68rem;"
        "color:#334155;margin-bottom:16px;'>Azure OpenAI · FastAPI · Streamlit</div>",
        unsafe_allow_html=True,
    )

    # ── Backend status ────────────────────────────────────────────────────────
    health = api_get("/", timeout=3)
    if health is None:
        st.error(f"✗ API offline — start FastAPI on {API_BASE}")
    elif health.get("data_loaded"):
        fname = health.get("file_name", "unknown")
        n_tables = health.get("table_count", 0)
        st.success(f"✓ API online · **{fname}** · {n_tables} table(s)")
        st.session_state.data_loaded = True
    else:
        st.warning("✓ API online · No file loaded yet")
        st.session_state.data_loaded = False

    st.divider()

    # ── File upload ───────────────────────────────────────────────────────────
    st.markdown("### 📂 Load Data File")
    st.caption("Accepts .xlsx, .xls, or .csv")
    uploaded = st.file_uploader(
        "Choose file",
        type=["xlsx", "xls", "csv"],
        label_visibility="collapsed",
    )
    if uploaded is not None:
        with st.spinner(f"Loading {uploaded.name}…"):
            result = upload_file(uploaded.read(), uploaded.name)

        if result and "error" not in result:
            st.success(f"✓ Loaded: **{result.get('file_name')}**")
            # Show table row counts
            for tname, rcount in result.get("tables", {}).items():
                st.caption(f"  `{tname}` — {rcount:,} rows")
            st.session_state.data_loaded = True
            invalidate_schema_cache()
            # Reset chat when new file is loaded
            st.session_state.messages = [
                {
                    "role": "assistant",
                    "content": (
                        f"✅ File **{result.get('file_name')}** loaded successfully.\n\n"
                        + "\n".join(
                            f"- `{t}`: {r:,} rows"
                            for t, r in result.get("tables", {}).items()
                        )
                        + "\n\nAsk me anything about this data."
                    ),
                    "sql": None, "validation": None, "results": None, "error": None,
                }
            ]
            st.rerun()
        else:
            st.error(f"Upload failed: {result.get('error', 'Unknown error')}")

    st.divider()

    # ── Schema / Relationships explorer ──────────────────────────────────────
    if st.session_state.data_loaded:
        sidebar_tab = st.radio(
            "Explore", ["Schema", "Relationships"],
            horizontal=True, label_visibility="collapsed",
        )

        if sidebar_tab == "Schema":
            schema = fetch_schema()
            if schema:
                search = st.text_input("Search tables / columns", placeholder="e.g. status, product…")
                tables = schema.get("tables", {})
                for tname, tmeta in tables.items():
                    q = search.lower()
                    cols = tmeta.get("columns", [])
                    if q and not (
                        q in tname.lower()
                        or any(q in c.lower() for c in cols)
                    ):
                        continue
                    with st.expander(f"**{tname}**  ·  {tmeta.get('row_count', 0):,} rows", expanded=False):
                        st.caption(f"Source: {tmeta.get('original_name', tname)}")
                        st.markdown(" · ".join(f"`{c}`" for c in cols))
            else:
                st.caption("Schema not available.")

        else:  # Relationships
            rels = fetch_relationships()
            if rels:
                st.caption(f"{len(rels)} inferred relationship(s)")
                for rel in rels:
                    st.markdown(
                        f"<div class='rel-row'>"
                        f"<span style='color:#38bdf8'>{rel['table_a']}</span>"
                        f".<span style='color:#7dd3fc'>{rel['column']}</span>"
                        f" = "
                        f"<span style='color:#a78bfa'>{rel['table_b']}</span>"
                        f".<span style='color:#c4b5fd'>{rel['column']}</span>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )
            else:
                st.caption("No shared columns detected between tables.")

    st.divider()

    # ── Controls ──────────────────────────────────────────────────────────────
    if st.button("🗑 Clear conversation", use_container_width=True):
        st.session_state.messages = [st.session_state.messages[0]]
        st.rerun()

    # ── Inline SQL Validator ──────────────────────────────────────────────────
    st.markdown("### 🔍 SQL Validator")
    raw_sql = st.text_area("Paste SQL to validate", height=90, placeholder="SELECT …")
    if st.button("Validate", use_container_width=True) and raw_sql.strip():
        result = api_post("/validate", {"sql": raw_sql})
        if result:
            if result.get("valid"):
                st.success("✓ Valid SQL")
            else:
                st.error("✗ Invalid SQL")
                for e in result.get("errors", []):
                    st.markdown(f"- {e}")
            for w in result.get("warnings", []):
                st.warning(w)


# ── Main area ─────────────────────────────────────────────────────────────────
st.markdown(
    "<h3 style='color:#f1f5f9;margin-bottom:2px;'>Data Query Assistant</h3>"
    "<div style='color:#334155;font-family:IBM Plex Mono,monospace;"
    "font-size:0.7rem;margin-bottom:14px;'>"
    "Azure OpenAI · SQLite · SQL-validated</div>",
    unsafe_allow_html=True,
)

# ── Suggested queries — generated from live schema, not hardcoded ─────────────
if st.session_state.data_loaded:
    schema = fetch_schema()
    if schema:
        tables = list(schema.get("tables", {}).keys())
        # Build generic but useful suggestions from actual table names
        suggestions: list[str] = []
        for t in tables[:4]:
            suggestions.append(f"Show me all rows in {t}")
        if len(tables) >= 2:
            suggestions.append(f"How many rows are in each table?")
            suggestions.append(f"Join {tables[0]} and {tables[1]} on their shared columns")
        suggestions.append("What columns does each table have?")

        cols_display = st.columns(min(len(suggestions), 4))
        for i, q in enumerate(suggestions):
            with cols_display[i % 4]:
                if st.button(q, key=f"sq_{i}"):
                    st.session_state.pending_query = q

st.divider()


# ── Message renderer ──────────────────────────────────────────────────────────
def render_message(msg: dict):
    # Guard: skip any malformed entries that lack role/content
    if not msg.get("role") or not isinstance(msg.get("content"), str):
        return

    is_user = msg["role"] == "user"
    label = "YOU" if is_user else "ASSISTANT"
    css   = "user-msg" if is_user else "assistant-msg"

    st.markdown(f'<div class="msg-label">{label}</div>', unsafe_allow_html=True)

    def _md_to_html(text: str) -> str:
        import re as _re, html as _html
        text = _html.escape(text)
        text = _re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', text, flags=_re.DOTALL)
        text = _re.sub(r'(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)', r'<em>\1</em>', text)
        text = _re.sub(r'`([^`]+)`', r'<code style="background:#1e293b;padding:1px 5px;border-radius:3px;font-size:0.85em;">\1</code>', text)
        text = _re.sub(r'(?m)^[-•]\s+(.+)$', r'&nbsp;&nbsp;• \1', text)
        text = text.replace('\n', '<br>')
        return text

    st.markdown(f'<div class="{css}">{_md_to_html(msg["content"])}</div>', unsafe_allow_html=True)

    if msg.get("sql"):
        with st.expander("View SQL", expanded=False):
            st.markdown(
                f'<div class="sql-block">{msg["sql"]}</div>',
                unsafe_allow_html=True,
            )
            v = msg.get("validation")
            if v:
                lines = ["<b>✓ SQL Valid</b>" if v["valid"] else "<b>✗ SQL Invalid</b>"]
                for e in v.get("errors", []):
                    lines.append(f"• {e}")
                for w in v.get("warnings", []):
                    lines.append(f'<span class="warn-line">⚠ {w}</span>')
                badge = "valid-badge" if v["valid"] else "invalid-badge"
                st.markdown(
                    f'<div class="{badge}">' + "<br>".join(lines) + "</div>",
                    unsafe_allow_html=True,
                )

    all_queries = msg.get("all_queries") or []
    if len(all_queries) > 1:
        with st.expander(f"🔍 Investigation trail — {len(all_queries)} queries run", expanded=False):
            for i, step in enumerate(all_queries, 1):
                st.markdown(
                    f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.68rem;"
                    f"color:#38bdf8;margin:10px 0 4px;'>QUERY {i}</div>",
                    unsafe_allow_html=True,
                )
                st.markdown(
                    f'<div class="sql-block">{step.get("sql", "")}</div>',
                    unsafe_allow_html=True,
                )
                if step.get("error"):
                    st.markdown(
                        f'<div class="error-block">✗ {step["error"]}</div>',
                        unsafe_allow_html=True,
                    )
                elif step.get("results") is not None:
                    rows = step["results"]
                    if rows:
                        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
                        st.caption(f"{len(rows):,} row{'s' if len(rows) != 1 else ''}")
                    else:
                        st.caption("No rows returned.")

    if msg.get("results") is not None:
        results = msg["results"]
        st.markdown(
            "<div style='font-family:IBM Plex Mono,monospace;font-size:0.68rem;"
            "color:#334155;margin:8px 0 4px;letter-spacing:0.07em;'>QUERY RESULTS</div>",
            unsafe_allow_html=True,
        )
        if results:
            df = pd.DataFrame(results)
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.caption(f"{len(results):,} row{'s' if len(results) != 1 else ''} returned")
        else:
            st.caption("No rows returned.")

    if msg.get("error"):
        st.markdown(
            f'<div class="error-block">✗ {msg["error"]}</div>',
            unsafe_allow_html=True,
        )


# ── Render history ────────────────────────────────────────────────────────────
for msg in st.session_state.messages:
    render_message(msg)

# ── Input form ────────────────────────────────────────────────────────────────
st.divider()

with st.form("chat_form", clear_on_submit=True):
    col_in, col_btn = st.columns([9, 1])
    with col_in:
        default = st.session_state.pop("pending_query", "") or ""
        user_input = st.text_area(
            "Question",
            value=default,
            placeholder="Ask a question about your data…",
            height=80,
            label_visibility="collapsed",
        )
    with col_btn:
        submitted = st.form_submit_button("↑ Send", use_container_width=True)

st.caption("Shift+Enter for newline · click Send to submit")

# ── Handle submission ─────────────────────────────────────────────────────────
if submitted and user_input.strip():
    if not st.session_state.data_loaded:
        st.warning("⚠️ Please upload a data file first using the sidebar.")
        st.stop()

    user_msg = {
        "role": "user", "content": user_input.strip(),
        "sql": None, "validation": None, "results": None, "error": None,
    }
    st.session_state.messages.append(user_msg)

    with st.spinner("Investigating — running queries, this may take a moment…"):
        response = api_post(
            "/chat",
            {
                "message": user_input.strip(),
                "history": [
                    {"role": m["role"], "content": m["content"]}
                    for m in st.session_state.messages[:-1]
                    if m.get("role") in ("user", "assistant") and m.get("content")
                ],
            },
        )

    if response:
        st.session_state.messages.append(response)

    st.rerun()
