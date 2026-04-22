import re
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import openai

from config import settings
from data_loader import DatabaseState, execute_query
from sql_validator import validate_sql

logger = logging.getLogger(__name__)

_SQL_PATTERN = re.compile(r"<sql>([\s\S]*?)</sql>", re.IGNORECASE)
_DONE_PATTERN = re.compile(r"<done\s*/>|<done></done>", re.IGNORECASE)

# ── Metadata ──────────────────────────────────────────────────────────────────

_META: dict = {}
for _meta_candidate in [
    Path(__file__).parent / "metadata.json",
    Path.cwd() / "metadata.json",
]:
    if _meta_candidate.exists():
        try:
            _META = json.loads(_meta_candidate.read_text(encoding="utf-8"))
            break
        except Exception as e:
            logger.warning("metadata.json load error (%s): %s", _meta_candidate, e)
if not _META:
    logger.warning("metadata.json not found — table descriptions and join hints unavailable.")

_TABLE_DESCRIPTIONS: dict[str, str] = {
    t: v.get("description", "")
    for t, v in _META.get("tables", {}).items()
}
_JOIN_NOTES: list[str] = _META.get("join_hints", {}).get("notes", [])


# ── Domain knowledge ──────────────────────────────────────────────────────────

_PHARMA_DOMAIN = """
You are an expert pharma field data assistant for a pharmaceutical sales team.
You understand the data, business processes, and terminology deeply.

DATA SOURCES:
- Xponent: retail prescriber-level Rx data. Volume attributed to individual HCP NPI. Standard dashboard view.
- DDD (Direct Data Dispensing): non-retail outlet-level data. Volume attributed to the HCO/account, NOT an individual prescriber. Used for specialty clinics, infusion centers, hospital outpatient departments. Reps won't see this in their standard Xponent view.
- 867 Shipment: distributor-to-account sell-IN event. Records product moving from a distributor to a ship-to location. This is NOT a dispense — no patient has received the product yet, and no prescriber NPI is attached. Typical lag from dispense to DDD appearance: 4-6 weeks.
- CRM: rep-facing system tracking calls, DNC flags, call frequency. Can lag behind IQVIA OneKey updates.
- IQVIA OneKey: master HCP/HCO reference — addresses, specialties, affiliations, DNC flags, credit flags.
- Alignment: maps ZIP codes to Territory/Region/Area. Determines rep ownership of HCPs.
- DCR (Data Change Request): formal submission to correct data issues.

COMMON FIELD REP ISSUES:
- Volume disappeared from dashboard: likely an HCP merge (two OneKey records consolidated, one retired — if surviving record maps to a different territory, rep loses credit) or HCP move (new ZIP maps to a different territory). Always check DCR log first, then OneKey record history, then Alignment.
- Shipment showing but no DDD volume: 867 is a sell-in event, not a dispense. DDD lags 4-6 weeks post-dispense. Also check if the account is non-retail (volume in DDD at outlet level, not Xponent at prescriber level).
- Can't find volume for an account: account may be non-retail — check DDD, not Xponent. If non-reporting, data may be suppressed or projected only.
- HCP has DNC flag but rep has been calling: check DNC scope. An email unsubscribe = email/digital channel only — does NOT restrict in-person visits unless HCP explicitly requested it in writing.
- IC credit missing: check Creditable and IC_Credit flags on the HCP record. Merges/address updates can reset these — verify they transferred to the surviving record.
- Target list changed unexpectedly: check Target_List_Refresh for version/refresh date, and check if the HCP's Alignment_Status changed.

IC CREDITING:
- Creditable specialties generate IC credit — verify Creditable = Yes and IC_Credit = Yes on the HCP record.
- Merges and address updates can reset credit flags — always verify on the surviving/current record.
- Non-retail IC credit requires the outlet ship-to to be mapped to the rep's territory in the 867 config.
- DNC flags do NOT affect IC credit eligibility — only contact channel permissions.

WHEN TO RECOMMEND A DCR:
- HCP merged to wrong territory, credit flag missing after merge, DNC flag applied too broadly (blanket instead of scoped), alignment mismatch, 867 ship-to mapped to wrong territory.
"""


def _build_schema_block(db_state: DatabaseState) -> str:
    lines = []
    for tname, tmeta in db_state.tables.items():
        cols = ", ".join(
            f'"{c}"' if cm.has_spaces else c
            for c, cm in zip(tmeta.column_names, tmeta.columns)
        )
        desc = _TABLE_DESCRIPTIONS.get(tname, "")
        lines.append(f"  {tname} — {desc}\n    Columns: {cols}")

    space_cols = [c for tmeta in db_state.tables.values() for c in tmeta.quoted_columns]
    space_note = (
        f"\n  ⚠ These column names contain spaces — always double-quote them in SQL: {', '.join(space_cols)}"
        if space_cols else ""
    )
    joins = "\n".join(f"  - {n}" for n in _JOIN_NOTES)

    return (
        f"DATABASE ({db_state.file_name}, {len(db_state.tables)} tables):\n"
        + "\n".join(lines)
        + space_note
        + (f"\n\nJOIN PATHS:\n{joins}" if joins else "")
    )


def _build_system_prompt(db_state: Optional[DatabaseState]) -> str:
    db_section = (
        f"\nLIVE DATABASE SCHEMA:\n{_build_schema_block(db_state)}\n"
        if db_state is not None
        else "\nNo data file loaded — answer conceptual questions from domain knowledge. For data queries, ask the user to upload a file.\n"
    )

    return f"""You are a pharma field data steward. Your job is to fully investigate rep inquiries yourself — running every query needed — and then deliver a single, complete, confirmed answer directly to the rep.

{_PHARMA_DOMAIN}

════════════════════════════════════════
HOW YOU WORK — AGENTIC INVESTIGATION LOOP
════════════════════════════════════════
You operate in a silent investigation loop. The rep never sees your intermediate steps — only your final answer. On each turn you may do ONE of two things:

(A) INVESTIGATE — run a query to gather a specific piece of evidence:
    • Write one short internal note: what you're checking and why (this will be hidden from the rep)
    • Emit exactly ONE <sql>...</sql> block
    • Do NOT write any rep-facing text yet
    • Do NOT emit <done /> — wait for the results before continuing

(B) CONCLUDE — deliver your final answer to the rep once you have enough evidence:
    • Write the complete, confirmed answer in the RESPONSE FORMAT below
    • Emit <done /> at the very end
    • Do NOT include any <sql> block in this same turn

INVESTIGATION RULES:
- You may run up to 6 queries. After that, conclude with what you have.
- Run queries in the right order: DCR log first → master record → prescribing data → credit flags → alignment.
- If a result comes back empty, that IS a finding — state what it means in business terms.
- Never ask the rep to check anything themselves.
- Never say "I would check X" or "you should verify Y" — you check it, you verify it.
- Never list investigation steps in the final answer.

════════════════════════════════════════
FINAL RESPONSE FORMAT  (rep-facing answer only)
════════════════════════════════════════
Write in first-person plural ("We investigated...", "We confirmed...", "We pulled...").
Structure your answer as a flowing narrative — short paragraphs and Bullet points Follow the Natural Flow mentioned."

Follow this natural flow:
  1. Open with what you confirmed happened (the root cause), stated as fact — not hypothesis. It should be 1-2 lines max, and it should directly answer the rep's question. Avoid hedging language.
     Example: "We investigated /We Analyzed/We Confirmed and followed up."
  2. The heading should be key Findings , in 3/4 Bullet point. Explain the mechanism in plain terms — what a merge/move/DNC/classification means and
     why it caused the symptom the rep is seeing.State the current data facts
  3. The headding should be Detailed Annalysis , in 3/4 Bullet point. State the credit/IC impact explicitly: is the rep getting credit, losing credit, or is it
     recoverable? Name the creditable specialties involved.State the current data facts
  4. If there is a recommended next action,keep it brief 2 linees max (e.g. check dashboard again on a specific date,
     contact Sales Ops): state it as one concrete sentence at the end. There should be no heading to it, it comes as a final note after the analysis.

TONE:
- Direct and confident — you ran the queries, you know the answer.
- Plain language — no jargon the rep doesn't know, but don't over-explain basics they do know.
- No hedging ("it appears", "it seems", "this may be") — state what the data shows.
- No preamble ("Great question!", "I'll look into this") — start with the finding.
- There should be no Tags Eg. <internal>,<external>, <sql>, <done> in the final answer — these are only for structuring your investigation process, not for display.

════════════════════════════════════════
SQL RULES
════════════════════════════════════════
- Wrap SQL in <sql>...</sql> tags
- SQLite only — SELECT only, no INSERT/UPDATE/DELETE/DROP/ALTER/CREATE
- Use table aliases; add LIMIT 100 unless user explicitly asks for all rows
- Double-quote any column name that contains spaces
- Only reference tables and columns that exist in the schema below

{db_section}"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def extract_sql(text: str) -> str | None:
    m = _SQL_PATTERN.search(text)
    return m.group(1).strip() if m else None


def is_done(text: str) -> bool:
    return bool(_DONE_PATTERN.search(text))


def _clean_text(text: str) -> str:
    """Strip SQL tags and <done /> from display text."""
    text = _SQL_PATTERN.sub("", text)
    text = _DONE_PATTERN.sub("", text)
    return text.strip()


def _format_results_for_context(sql: str, results: list[dict], error: str | None) -> str:
    """Render query results as a compact context block to feed back to the LLM."""
    if error:
        return (
            f"[INTERNAL — NOT SHOWN TO REP]\n"
            f"QUERY RESULT (error):\n{error}\n"
            f"SQL: {sql}\n"
            f"Interpret this error in business terms, then run another query or conclude."
        )
    if not results:
        return (
            f"[INTERNAL — NOT SHOWN TO REP]\n"
            f"QUERY RESULT: 0 rows returned.\n"
            f"SQL: {sql}\n"
            f"No rows means something specific — state what it implies, then continue investigating or conclude."
        )
    sample = results[:50]
    rows_text = json.dumps(sample, default=str, indent=2)
    note = f" (showing first 50 of {len(results)})" if len(results) > 50 else ""
    return (
        f"[INTERNAL — NOT SHOWN TO REP]\n"
        f"QUERY RESULT ({len(results)} rows{note}):\n{rows_text}\n"
        f"SQL: {sql}\n"
        f"Use these results to inform your next query or, if you have enough, write your final rep-facing answer and emit <done />."
    )


# ── Response dataclass ────────────────────────────────────────────────────────

@dataclass
class AgentResponse:
    role:       str               = "assistant"
    content:    str               = ""
    sql:        str | None        = None          # last SQL executed (for display)
    validation: dict | None       = None
    results:    list[dict] | None = None          # last query results (for display)
    all_queries: list[dict]       = None          # all steps [{sql, results, error}]
    error:      str | None        = None

    def __post_init__(self):
        if self.all_queries is None:
            self.all_queries = []

    def to_dict(self) -> dict:
        return {
            "role":        self.role,
            "content":     self.content,
            "sql":         self.sql,
            "validation":  self.validation,
            "results":     self.results,
            "all_queries": self.all_queries,
            "error":       self.error,
        }


# ── Agent ─────────────────────────────────────────────────────────────────────

class SQLAgent:
    MAX_ITERATIONS = 6

    def __init__(self) -> None:
        self._client = openai.AsyncAzureOpenAI(
            api_key=settings.azure_openai_api_key,
            azure_endpoint=settings.azure_openai_endpoint,
            api_version=settings.azure_openai_api_version,
        )

    def _base_messages(
        self,
        user_text: str,
        history: list[dict[str, Any]] | None,
        db_state: Optional[DatabaseState],
    ) -> list[dict]:
        messages = [{"role": "system", "content": _build_system_prompt(db_state)}]

        safe = [
            {"role": m["role"], "content": m["content"]}
            for m in (history or [])
            if m.get("role") in ("user", "assistant") and str(m.get("content", "")).strip()
        ]
        while safe and safe[0]["role"] == "assistant":
            safe.pop(0)

        cleaned: list[dict] = []
        for msg in safe:
            if cleaned and cleaned[-1]["role"] == msg["role"]:
                continue
            cleaned.append(msg)

        messages.extend(cleaned)
        messages.append({"role": "user", "content": user_text})
        return messages

    async def run(
        self,
        user_text: str,
        history: list[dict[str, Any]] | None = None,
        db_state: Optional[DatabaseState] = None,
    ) -> AgentResponse:
        if not user_text.strip():
            return AgentResponse(content="Please enter a question.")

        try:
            # Conversation thread that grows with each tool-use round-trip
            messages = self._base_messages(user_text, history, db_state)

            all_queries: list[dict] = []
            final_content: str = ""
            last_sql: str | None = None
            last_results: list[dict] | None = None
            last_validation: dict | None = None
            last_error: str | None = None

            for iteration in range(self.MAX_ITERATIONS):
                response = await self._client.chat.completions.create(
                    model=settings.azure_openai_deployment,
                    max_tokens=settings.openai_max_tokens,
                    messages=messages,
                )

                raw_text = response.choices[0].message.content or ""
                sql = extract_sql(raw_text)
                done = is_done(raw_text)

                # Always append the assistant turn to the thread
                messages.append({"role": "assistant", "content": raw_text})

                if done or (not sql):
                    # Final answer — no more queries needed (or max iterations hit)
                    final_content = _clean_text(raw_text)
                    break

                # ── Execute the SQL ───────────────────────────────────────────
                step_error: str | None = None
                step_results: list[dict] | None = None
                step_validation: dict | None = None

                if db_state is None:
                    step_error = "No data file loaded — cannot execute SQL."
                else:
                    vr = (
                        validate_sql(sql, db_state)
                        if settings.sql_validation_enabled
                        else validate_sql(sql, None)
                    )
                    step_validation = vr.to_dict()

                    if vr.valid:
                        try:
                            step_results = execute_query(
                                vr.sanitized or sql,
                                limit=settings.query_row_limit,
                            )
                            logger.info(
                                "[iter %d] Query returned %d rows", iteration, len(step_results)
                            )
                        except Exception as exc:
                            step_error = f"SQL execution error: {exc}"
                            logger.warning("[iter %d] Execution error: %s", iteration, exc)
                    else:
                        step_error = "SQL validation failed: " + "; ".join(vr.errors)

                # Record this step
                all_queries.append(
                    {
                        "sql":        sql,
                        "results":    step_results,
                        "validation": step_validation,
                        "error":      step_error,
                    }
                )

                # Keep track of last successful query for top-level response fields
                last_sql        = sql
                last_validation = step_validation
                if step_results is not None:
                    last_results = step_results
                if step_error:
                    last_error = step_error

                # ── Feed results back as a user turn so the LLM can continue ─
                context_msg = _format_results_for_context(sql, step_results or [], step_error)
                messages.append({"role": "user", "content": context_msg})

            else:
                # Fell through the loop — force a synthesis pass
                messages.append({
                    "role": "user",
                    "content": (
                        "You have run the maximum number of queries. "
                        "Using all the results above, deliver your final rep-facing answer now. "
                        "Follow the response format exactly: confirmed findings stated as fact, "
                        "plain narrative paragraphs, specific data points (NPIs, dates, territories, "
                        "units, credit flags), IC/credit impact, and DCR recommendation if needed. "
                        "No numbered steps. No hedging. End with <done />."
                    ),
                })
                response = await self._client.chat.completions.create(
                    model=settings.azure_openai_deployment,
                    max_tokens=settings.openai_max_tokens,
                    messages=messages,
                )
                raw_text = response.choices[0].message.content or ""
                final_content = _clean_text(raw_text)

            return AgentResponse(
                content=final_content,
                sql=last_sql,
                validation=last_validation,
                results=last_results,
                all_queries=all_queries,
                error=last_error if not final_content else None,
            )

        except openai.AuthenticationError:
            return AgentResponse(
                content="⚠️ Invalid Azure OpenAI credentials. Check AZURE_OPENAI_API_KEY and AZURE_OPENAI_ENDPOINT.",
                error="AuthenticationError",
            )
        except openai.RateLimitError:
            return AgentResponse(
                content="⚠️ Rate limit exceeded. Please wait and try again.",
                error="RateLimitError",
            )
        except openai.NotFoundError:
            return AgentResponse(
                content=f"⚠️ Deployment '{settings.azure_openai_deployment}' not found. Check AZURE_OPENAI_DEPLOYMENT.",
                error="NotFoundError",
            )
        except Exception as exc:
            logger.exception("Unexpected agent error")
            return AgentResponse(content="⚠️ An unexpected error occurred.", error=str(exc))
