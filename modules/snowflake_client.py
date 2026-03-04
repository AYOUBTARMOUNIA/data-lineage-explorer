"""
snowflake_client.py — Session Snowflake + helpers SQL
Compatible Streamlit in Snowflake (get_active_session)
"""
import streamlit as st
import pandas as pd
from datetime import datetime

try:
    from snowflake.snowpark.context import get_active_session
    _SIS_MODE = True
except ImportError:
    _SIS_MODE = False


# ── Session ──────────────────────────────────────────────────────────────────

@st.cache_resource(show_spinner=False)
def get_session():
    """Retourne la session Snowpark active (injectée par Snowflake SiS)."""
    if _SIS_MODE:
        return get_active_session()
    raise RuntimeError("Snowflake session non disponible en dehors de SiS.")


# ── SQL helpers ───────────────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def run_sql(query: str, params: dict | None = None) -> pd.DataFrame:
    """
    Exécute une requête SELECT et retourne un DataFrame.
    READ-ONLY : toute tentative DDL/DML lève une exception.
    """
    _guard_readonly(query)
    session = get_session()
    try:
        if params:
            # Substitution manuelle sécurisée (paramètres nommés :key)
            safe_query = _bind_params(query, params)
        else:
            safe_query = query
        return session.sql(safe_query).to_pandas()
    except Exception as e:
        _log_error(str(e), query)
        raise


def run_sql_no_cache(query: str, params: dict | None = None) -> pd.DataFrame:
    """Version sans cache (pour les requêtes d'audit / temps-réel)."""
    _guard_readonly(query)
    session = get_session()
    safe_query = _bind_params(query, params) if params else query
    return session.sql(safe_query).to_pandas()


# ── Sécurité READ-ONLY ────────────────────────────────────────────────────────

_FORBIDDEN_KEYWORDS = {
    "INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE",
    "CREATE", "ALTER", "DROP", "REPLACE", "COPY INTO",
    "GRANT", "REVOKE", "CALL", "EXECUTE",
}


def _guard_readonly(query: str):
    first_token = query.strip().split()[0].upper() if query.strip() else ""
    if first_token in _FORBIDDEN_KEYWORDS:
        raise PermissionError(
            f"❌ Requête interdite ({first_token}). "
            "L'application est en mode READ-ONLY."
        )


def _bind_params(query: str, params: dict) -> str:
    """Substitution sécurisée des paramètres nommés (:key → valeur)."""
    result = query
    for key, val in params.items():
        if isinstance(val, str):
            safe_val = val.replace("'", "''")
            result = result.replace(f":{key}", f"'{safe_val}'")
        else:
            result = result.replace(f":{key}", str(val))
    return result


# ── Audit log ─────────────────────────────────────────────────────────────────

def log_action(
    module: str,
    action: str,
    object_name: str = "",
    details: str = "",
    db: str = "DATA_HUB",
    schema: str = "APP_CONFIG",
):
    """
    Journalise une action applicative dans la table d'audit Snowflake.
    Fail-silent : une erreur de log ne doit pas bloquer l'UX.
    """
    try:
        session = get_session()
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        detail_safe = details.replace("'", "''")[:2000]
        obj_safe = object_name.replace("'", "''")[:500]
        session.sql(f"""
            INSERT INTO {db}.{schema}.APP_AUDIT_LOG
                (EVENT_TIMESTAMP, MODULE, ACTION, OBJECT_NAME, DETAILS, SESSION_USER)
            VALUES (
                '{ts}', '{module}', '{action}', '{obj_safe}',
                '{detail_safe}',
                CURRENT_USER()
            )
        """).collect()
    except Exception:
        pass  # Fail-silent


def _log_error(error: str, query: str):
    log_action("snowflake_client", "SQL_ERROR", details=f"{error[:200]} | {query[:200]}")
