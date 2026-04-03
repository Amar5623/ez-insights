"""
core/client_config.py

Loads and caches the client-specific configuration bundle at startup.

Every client deployment has its own config folder containing:
  - client.yaml          → identity, tone, business description
  - prompts/sql_system.md    → SQL generation system prompt (with {{ }} placeholders)
  - prompts/answer_system.md → NL answer generation system prompt
  - db_context.yaml      → rich table/column descriptions for schema indexing

The loader reads these once at startup and exposes a typed ClientConfig
object. All downstream code (PromptBuilder, SchemaRetriever) reads from
this object — never from hardcoded strings.

USAGE:
    from core.client_config import get_client_config
    cfg = get_client_config()

    # In PromptBuilder:
    system_prompt = cfg.sql_system_prompt     # fully rendered string
    db_context    = cfg.db_context_markdown   # static markdown for prompt injection

    # In SchemaRetriever:
    enriched_chunks = cfg.get_enriched_schema_chunks()  # for FAISS indexing

SWITCHING CLIENTS:
    Set CLIENT_CONFIG_PATH=./client-configs/<client_name> in .env.
    Call get_client_config.cache_clear() in tests to reload.

TEMPLATE VARIABLES:
    Prompt files support {{ variable }} placeholders.
    Available variables: all keys from client.yaml (assistant_name, company_name, etc.)
    The loader renders these at load time — no runtime substitution needed.
"""

import os
import re
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Optional

import yaml

from core.logging_config import get_logger

logger = get_logger(__name__)


# ─── Data model ───────────────────────────────────────────────────────────────


@dataclass
class ClientConfig:
    """
    Fully loaded and rendered client configuration.

    All string fields are ready to use — template variables already substituted.
    No further processing needed by callers.
    """

    # Identity (from client.yaml)
    assistant_name: str
    company_name: str
    company_short: str
    db_name: str
    db_type: str
    tone: str
    business_description: str
    currency_symbol: str
    currency_code: str
    date_format: str
    in_scope_description: str
    out_of_scope_description: str

    # Rendered prompts (from prompts/*.md, template vars substituted)
    sql_system_prompt: str
    answer_system_prompt: str

    # ❗ FIX: moved BEFORE default field
    db_context_markdown: str

    # Optional fields (default AFTER all required fields)
    mongo_system_prompt: Optional[str] = None

    # Structured DB context
    db_context_structured: dict = field(default_factory=dict)

    def get_enriched_schema_chunks(self) -> list[dict]:
        """
        Return a list of enriched text chunks — one per table — for FAISS indexing.

        These chunks are RICHER than raw DESCRIBE output because they include:
          - Business description of what the table represents
          - Valid values for enum-like columns (status, territory, productLine, etc.)
          - Join notes (how this table connects to others)
          - Common query patterns (what users typically ask about it)

        SchemaRetriever indexes these alongside the raw schema chunks.
        The enriched chunks get a higher retrieval weight because they contain
        the vocabulary users actually use ("revenue", "late orders", etc.).

        Returns:
            list of dicts: [{"entity": table_name, "schema_text": rich_text}, ...]
        """
        tables = self.db_context_structured.get("tables", {})
        chunks = []

        for table_name, table_info in tables.items():
            if not isinstance(table_info, dict):
                continue

            lines = [f"Table: {table_name}"]

            desc = table_info.get("description", "")
            if desc:
                lines.append(f"Description: {desc.strip()}")

            # Key columns with valid values
            key_cols = table_info.get("key_columns", {})
            if key_cols:
                lines.append("Key columns:")
                for col_name, col_info in key_cols.items():
                    if not isinstance(col_info, dict):
                        continue
                    col_desc = col_info.get("description", "")
                    valid_vals = col_info.get("valid_values", [])
                    line = f"  - {col_name}: {col_desc.strip()}"
                    if valid_vals:
                        line += f" [values: {', '.join(str(v) for v in valid_vals)}]"
                    lines.append(line)

            # Join notes
            join_notes = table_info.get("join_notes", "")
            if join_notes:
                lines.append(f"Joins: {join_notes.strip()}")

            # Common query patterns — this is the most valuable for retrieval
            common = table_info.get("common_queries", [])
            if common:
                lines.append("Common queries: " + "; ".join(common))

            chunk_text = "\n".join(lines)
            chunks.append({
                "entity": f"context_{table_name}",   # prefix avoids collision with raw schema
                "schema_text": chunk_text,
                "is_enriched": True,
            })

            logger.debug(
                f"[CLIENT_CONFIG] Enriched chunk for '{table_name}': "
                f"{len(chunk_text)} chars"
            )

        return chunks


# ─── Loader ───────────────────────────────────────────────────────────────────

def _render_template(text: str, variables: dict) -> str:
    """
    Replace {{ variable_name }} placeholders in text with values from variables dict.

    Simple regex substitution — no Jinja2 dependency needed for this use case.
    Unknown variables are left as-is with a warning logged.
    """
    def replacer(match):
        key = match.group(1).strip()
        if key in variables:
            return str(variables[key])
        logger.warning(
            f"[CLIENT_CONFIG] Template variable '{{{{ {key} }}}}' not found in client.yaml"
        )
        return match.group(0)  # leave unchanged

    return re.sub(r"\{\{\s*(\w+)\s*\}\}", replacer, text)


def _load_yaml(path: Path) -> dict:
    """Load a YAML file and return its contents as a dict."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _load_text(path: Path) -> str:
    """Load a text/markdown file and return its contents as a string."""
    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip()


def _yaml_to_markdown(db_context: dict) -> str:
    """
    Convert the structured db_context.yaml into a concise markdown block
    suitable for injection into the SQL generation user message.

    This is the 'static DB context' section that every SQL prompt includes.
    It gives the LLM a quick reference card: tables, purposes, FK chain,
    safe views, and computed expressions.
    """
    lines = [f"## DATABASE: {db_context.get('database_name', 'unknown')}", ""]

    # Safe views
    views = db_context.get("views", {})
    if views:
        lines.append("### Safe Views (use by default)")
        for view_name, view_info in views.items():
            excludes = ", ".join(view_info.get("excludes_columns", []))
            lines.append(f"- `{view_name}`: replaces `{view_info.get('replaces', '?')}` — excludes: {excludes}")
        lines.append("")

    # Tables summary
    tables = db_context.get("tables", {})
    if tables:
        lines.append("### Tables")
        lines.append("| Table | Purpose |")
        lines.append("|-------|---------|")
        for table_name, table_info in tables.items():
            desc = table_info.get("description", "")
            # One-liner: take first sentence
            first_sentence = desc.strip().split(".")[0] + "." if desc else ""
            lines.append(f"| {table_name} | {first_sentence} |")
        lines.append("")

    # Key column notes (valid values — most important for accuracy)
    lines.append("### Key Column Valid Values")
    for table_name, table_info in tables.items():
        if not isinstance(table_info, dict):
            continue
        for col_name, col_info in table_info.get("key_columns", {}).items():
            if not isinstance(col_info, dict):
                continue
            valid_vals = col_info.get("valid_values", [])
            if valid_vals:
                lines.append(
                    f"- `{table_name}.{col_name}`: {', '.join(repr(v) for v in valid_vals)}"
                )
    lines.append("")

    # FK chain
    fk = db_context.get("fk_chain", "")
    if fk:
        lines.append("### FK Chain")
        lines.append(fk.strip())
        lines.append("")

    # Computed expressions
    exprs = db_context.get("computed_expressions", {})
    if exprs:
        lines.append("### Common Computed Expressions")
        for name, expr in exprs.items():
            lines.append(f"- **{name}**: `{expr}`")

    return "\n".join(lines)


def _load_client_config(config_path: str) -> ClientConfig:
    """
    Read the entire client config bundle from disk and return a ClientConfig.

    Args:
        config_path: Absolute or relative path to the client config folder.
                     e.g. "./client-configs/classicmodels"

    Raises:
        FileNotFoundError: If the config path or any required file is missing.
        ValueError: If client.yaml is missing required fields.
    """
    base = Path(config_path).resolve()

    logger.info(f"[CLIENT_CONFIG] Loading client config from: {base}")

    if not base.exists():
        raise FileNotFoundError(
            f"CLIENT_CONFIG_PATH does not exist: {base}\n"
            f"Create the folder and add client.yaml, prompts/, and db_context.yaml."
        )

    # ── 1. Load client.yaml ───────────────────────────────────────────────────
    client_yaml_path = base / "client.yaml"
    if not client_yaml_path.exists():
        raise FileNotFoundError(f"Missing required file: {client_yaml_path}")

    meta = _load_yaml(client_yaml_path)
    logger.info(
        f"[CLIENT_CONFIG] Loaded client.yaml | "
        f"client={meta.get('company_name', '?')} | "
        f"db={meta.get('db_name', '?')}"
    )

    # Validate required fields
    required_fields = ["assistant_name", "company_name", "db_name", "db_type"]
    for f_name in required_fields:
        if not meta.get(f_name):
            raise ValueError(
                f"client.yaml is missing required field: '{f_name}'. "
                f"Check {client_yaml_path}"
            )

    # Flatten multi-line YAML strings
    template_vars = {k: str(v).strip() for k, v in meta.items() if isinstance(v, (str, int, float))}

    # ── 2. Load SQL system prompt ─────────────────────────────────────────────
    sql_prompt_path = base / "prompts" / "sql_system.md"
    if not sql_prompt_path.exists():
        raise FileNotFoundError(f"Missing required file: {sql_prompt_path}")

    sql_system_raw = _load_text(sql_prompt_path)
    sql_system_prompt = _render_template(sql_system_raw, template_vars)
    logger.info(f"[CLIENT_CONFIG] Loaded sql_system.md ({len(sql_system_prompt)} chars)")

    # ── 3. Load answer system prompt ─────────────────────────────────────────
    answer_prompt_path = base / "prompts" / "answer_system.md"
    if not answer_prompt_path.exists():
        raise FileNotFoundError(f"Missing required file: {answer_prompt_path}")

    answer_system_raw = _load_text(answer_prompt_path)
    answer_system_prompt = _render_template(answer_system_raw, template_vars)
    logger.info(f"[CLIENT_CONFIG] Loaded answer_system.md ({len(answer_system_prompt)} chars)")

    # ── 4. Load db_context.yaml ───────────────────────────────────────────────
    db_context_yaml_path = base / "db_context.yaml"
    if not db_context_yaml_path.exists():
        raise FileNotFoundError(f"Missing required file: {db_context_yaml_path}")

    db_context_structured = _load_yaml(db_context_yaml_path)
    db_context_markdown = _yaml_to_markdown(db_context_structured)
    logger.info(
        f"[CLIENT_CONFIG] Loaded db_context.yaml | "
        f"tables={len(db_context_structured.get('tables', {}))} | "
        f"rendered_markdown={len(db_context_markdown)} chars"
    )

    # ── 5. Assemble and return ─────────────────────────────────────────────────
    config = ClientConfig(
        assistant_name=meta.get("assistant_name", "Analytics Assistant"),
        company_name=meta.get("company_name", ""),
        company_short=meta.get("company_short", meta.get("company_name", "")),
        db_name=meta.get("db_name", ""),
        db_type=meta.get("db_type", "mysql"),
        tone=meta.get("tone", "professional"),
        business_description=meta.get("business_description", ""),
        currency_symbol=meta.get("currency_symbol", "$"),
        currency_code=meta.get("currency_code", "USD"),
        date_format=meta.get("date_format", "MMM D, YYYY"),
        in_scope_description=meta.get("in_scope_description", ""),
        out_of_scope_description=meta.get("out_of_scope_description", ""),
        sql_system_prompt=sql_system_prompt,
        answer_system_prompt=answer_system_prompt,
        db_context_markdown=db_context_markdown,
        db_context_structured=db_context_structured,
    )

    enriched_chunks = config.get_enriched_schema_chunks()
    logger.info(
        f"[CLIENT_CONFIG] Ready | "
        f"enriched_chunks_available={len(enriched_chunks)} | "
        f"client='{config.company_name}'"
    )

    return config


# ─── Public API ───────────────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def get_client_config() -> ClientConfig:
    """
    Return the cached ClientConfig for the current deployment.

    Reads CLIENT_CONFIG_PATH from settings. Loads and renders once,
    then caches for the lifetime of the process.

    In tests: call get_client_config.cache_clear() after changing
    CLIENT_CONFIG_PATH to reload with a different client bundle.

    Raises:
        FileNotFoundError: If CLIENT_CONFIG_PATH is not set or doesn't exist.
        ValueError: If client.yaml is malformed.
    """
    from core.config.settings import get_settings
    s = get_settings()

    config_path = getattr(s, "CLIENT_CONFIG_PATH", None)
    if not config_path:
        raise ValueError(
            "CLIENT_CONFIG_PATH is not set in .env. "
            "Example: CLIENT_CONFIG_PATH=./client-configs/classicmodels"
        )

    return _load_client_config(config_path)