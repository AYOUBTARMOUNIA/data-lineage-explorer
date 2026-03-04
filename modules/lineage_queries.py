"""
lineage_queries.py — Toutes les requêtes SQL pour le Data Lineage
Sources: ACCOUNT_USAGE.OBJECT_DEPENDENCIES, INFORMATION_SCHEMA, QUERY_HISTORY
"""
import pandas as pd
from modules.snowflake_client import run_sql, run_sql_no_cache, get_session


# ══════════════════════════════════════════════════════════════════════════════
# NAVIGATION : DB / Schema / Objects
# ══════════════════════════════════════════════════════════════════════════════

def _col(df: pd.DataFrame, name: str) -> str:
    """
    Retourne le nom de colonne exact, insensible à la casse et aux
    guillemets que Snowpark ajoute parfois autour des noms (ex: '"name"').
    """
    name_lower = name.lower().strip('"')
    for c in df.columns:
        if c.lower().strip('"') == name_lower:
            return c
    raise KeyError(f"Colonne '{name}' introuvable dans {list(df.columns)}")


def get_databases() -> list[str]:
    """Liste toutes les bases de données accessibles."""
    df = run_sql("SHOW DATABASES")
    if df.empty:
        return []
    col = _col(df, "name")
    return sorted(df[col].tolist())


def get_schemas(database: str) -> list[str]:
    """Liste les schémas d'une base de données."""
    df = run_sql(f"SHOW SCHEMAS IN DATABASE {database}")
    if df.empty:
        return []
    col = _col(df, "name")
    exclude = {"INFORMATION_SCHEMA", "PUBLIC"}
    return sorted([s for s in df[col].tolist() if s.upper() not in exclude])


def get_objects(database: str, schema: str) -> pd.DataFrame:
    """Liste les tables et vues d'un schéma avec métadonnées."""
    df = run_sql(f"""
        SELECT
            TABLE_NAME       AS OBJECT_NAME,
            TABLE_TYPE       AS OBJECT_TYPE,
            ROW_COUNT,
            BYTES,
            LAST_ALTERED,
            COMMENT
        FROM {database}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema}'
          AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')
        ORDER BY TABLE_TYPE DESC, TABLE_NAME
    """)
    return df


# ══════════════════════════════════════════════════════════════════════════════
# LINEAGE OBJET — SNOWFLAKE.CORE.GET_LINEAGE (fonction native Snowflake)
# Fallback : ACCOUNT_USAGE.OBJECT_DEPENDENCIES (sans récursion)
# ══════════════════════════════════════════════════════════════════════════════

def _parse_lineage_df(df: pd.DataFrame, direction: str) -> pd.DataFrame:
    """
    Normalise le résultat de GET_LINEAGE vers le format interne :
    SRC_DB, SRC_SCHEMA, SRC_OBJECT, SRC_TYPE, TGT_DB, TGT_SCHEMA,
    TGT_OBJECT, TGT_TYPE, DEPTH, CONFIDENCE
    """
    if df.empty:
        return df

    # Normaliser les noms de colonnes (GET_LINEAGE retourne des majuscules)
    df.columns = [c.upper().strip('"') for c in df.columns]

    rows = []
    for _, row in df.iterrows():
        src_full = str(row.get("SOURCE_OBJECT_NAME", ""))
        tgt_full = str(row.get("TARGET_OBJECT_NAME", ""))
        src_parts = src_full.split(".")
        tgt_parts = tgt_full.split(".")

        rows.append({
            "SRC_DB":     src_parts[0] if len(src_parts) > 2 else "",
            "SRC_SCHEMA": src_parts[1] if len(src_parts) > 2 else "",
            "SRC_OBJECT": src_parts[-1],
            "SRC_TYPE":   str(row.get("SOURCE_OBJECT_DOMAIN", "TABLE")).upper(),
            "TGT_DB":     tgt_parts[0] if len(tgt_parts) > 2 else "",
            "TGT_SCHEMA": tgt_parts[1] if len(tgt_parts) > 2 else "",
            "TGT_OBJECT": tgt_parts[-1],
            "TGT_TYPE":   str(row.get("TARGET_OBJECT_DOMAIN", "TABLE")).upper(),
            "DEPTH":      int(row.get("DISTANCE", 1)),
            "CONFIDENCE": "CERTAIN",
        })
    return pd.DataFrame(rows)


def _exec(sql: str) -> pd.DataFrame:
    """Exécute directement via session (bypass cache — obligatoire pour lineage)."""
    session = get_session()
    df = session.sql(sql).to_pandas()
    df.columns = [c.upper().strip('"') for c in df.columns]
    return df


def get_upstream_dependencies(
    database: str,
    schema: str,
    object_name: str,
    max_depth: int = 3,
) -> tuple[pd.DataFrame, list[str]]:
    """
    Lineage upstream — sans cache, session directe.
    Essaie dans l'ordre :
      1. SNOWFLAKE.CORE.GET_LINEAGE  (natif Snowflake, args positionnels)
      2. ACCOUNT_USAGE.OBJECT_DEPENDENCIES (fallback)
    """
    full_name = f"{database}.{schema}.{object_name}"
    errors: list[str] = []

    # ── 1 : GET_LINEAGE ───────────────────────────────────────────────────────
    # Signature réelle : GET_LINEAGE(object_name, object_domain, direction, distance)
    for domain in ("Table", "View"):
        try:
            df = _exec(f"""
                SELECT SOURCE_OBJECT_DOMAIN, SOURCE_OBJECT_NAME,
                       TARGET_OBJECT_DOMAIN, TARGET_OBJECT_NAME, DISTANCE
                FROM TABLE(SNOWFLAKE.CORE.GET_LINEAGE(
                    '{full_name}', '{domain}', 'upstream', {max_depth}
                ))
            """)
            if not df.empty:
                return _parse_lineage_df(df, "upstream"), []
        except Exception as e:
            msg = str(e)
            if "Unknown domain" in msg:
                errors.append(f"GET_LINEAGE: objet '{object_name}' non supporté "
                               f"(Native App ou objet sans lineage enregistré)")
                break   # inutile de réessayer avec d'autres domains
            errors.append(f"GET_LINEAGE({domain}): {msg[:200]}")

    # ── 2 : OBJECT_DEPENDENCIES ───────────────────────────────────────────────
    try:
        df = _exec(f"""
            SELECT
                REFERENCED_DATABASE       AS SRC_DB,
                REFERENCED_SCHEMA         AS SRC_SCHEMA,
                REFERENCED_OBJECT_NAME    AS SRC_OBJECT,
                REFERENCED_OBJECT_DOMAIN  AS SRC_TYPE,
                REFERENCING_DATABASE      AS TGT_DB,
                REFERENCING_SCHEMA        AS TGT_SCHEMA,
                REFERENCING_OBJECT_NAME   AS TGT_OBJECT,
                REFERENCING_OBJECT_DOMAIN AS TGT_TYPE,
                1         AS DEPTH,
                'CERTAIN' AS CONFIDENCE
            FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
            WHERE UPPER(REFERENCING_DATABASE)   = UPPER('{database}')
              AND UPPER(REFERENCING_SCHEMA)      = UPPER('{schema}')
              AND UPPER(REFERENCING_OBJECT_NAME) = UPPER('{object_name}')
            ORDER BY SRC_OBJECT
        """)
        return df, errors
    except Exception as e:
        errors.append(f"OBJECT_DEPENDENCIES: {str(e)[:200]}")

    return pd.DataFrame(), errors


def get_downstream_dependencies(
    database: str,
    schema: str,
    object_name: str,
    max_depth: int = 3,
) -> tuple[pd.DataFrame, list[str]]:
    """Lineage downstream — sans cache, session directe."""
    full_name = f"{database}.{schema}.{object_name}"
    errors: list[str] = []

    # ── 1 : GET_LINEAGE ───────────────────────────────────────────────────────
    for domain in ("Table", "View"):
        try:
            df = _exec(f"""
                SELECT SOURCE_OBJECT_DOMAIN, SOURCE_OBJECT_NAME,
                       TARGET_OBJECT_DOMAIN, TARGET_OBJECT_NAME, DISTANCE
                FROM TABLE(SNOWFLAKE.CORE.GET_LINEAGE(
                    '{full_name}', '{domain}', 'downstream', {max_depth}
                ))
            """)
            if not df.empty:
                return _parse_lineage_df(df, "downstream"), []
        except Exception as e:
            msg = str(e)
            if "Unknown domain" in msg:
                errors.append(f"GET_LINEAGE: objet '{object_name}' non supporté "
                               f"(Native App ou objet sans lineage enregistré)")
                break
            errors.append(f"GET_LINEAGE({domain}): {msg[:200]}")

    # ── 2 : OBJECT_DEPENDENCIES ───────────────────────────────────────────────
    try:
        df = _exec(f"""
            SELECT
                REFERENCED_DATABASE       AS SRC_DB,
                REFERENCED_SCHEMA         AS SRC_SCHEMA,
                REFERENCED_OBJECT_NAME    AS SRC_OBJECT,
                REFERENCED_OBJECT_DOMAIN  AS SRC_TYPE,
                REFERENCING_DATABASE      AS TGT_DB,
                REFERENCING_SCHEMA        AS TGT_SCHEMA,
                REFERENCING_OBJECT_NAME   AS TGT_OBJECT,
                REFERENCING_OBJECT_DOMAIN AS TGT_TYPE,
                1         AS DEPTH,
                'CERTAIN' AS CONFIDENCE
            FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
            WHERE UPPER(REFERENCED_DATABASE)   = UPPER('{database}')
              AND UPPER(REFERENCED_SCHEMA)      = UPPER('{schema}')
              AND UPPER(REFERENCED_OBJECT_NAME) = UPPER('{object_name}')
            ORDER BY TGT_OBJECT
        """)
        return df, errors
    except Exception as e:
        errors.append(f"OBJECT_DEPENDENCIES: {str(e)[:200]}")

    return pd.DataFrame(), errors


# ══════════════════════════════════════════════════════════════════════════════
# LINEAGE COLONNE — ACCOUNT_USAGE.ACCESS_HISTORY (heuristique)
# ══════════════════════════════════════════════════════════════════════════════

def get_column_lineage(
    database: str,
    schema: str,
    object_name: str,
    limit: int = 200,
) -> pd.DataFrame:
    """
    Lineage colonne via ACCESS_HISTORY.
    Retourne les colonnes lues (upstream) et les colonnes écrites (downstream)
    associées à l'objet, avec leur fréquence d'accès.
    Confiance : PROBABLE (basé sur historique de requêtes).
    """
    df = run_sql(f"""
        SELECT
            obj.value:objectName::STRING                       AS OBJECT_FULL_NAME,
            obj.value:objectDomain::STRING                     AS OBJECT_TYPE,
            col.value:columnName::STRING                       AS COLUMN_NAME,
            COUNT(*)                                           AS ACCESS_COUNT,
            MAX(ah.QUERY_START_TIME)                           AS LAST_ACCESSED,
            'PROBABLE'                                         AS CONFIDENCE
        FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
             LATERAL FLATTEN(input => ah.BASE_OBJECTS_ACCESSED)   obj,
             LATERAL FLATTEN(input => obj.value:columns, outer => TRUE) col
        WHERE UPPER(obj.value:objectName::STRING)
              LIKE UPPER('%{database}.{schema}.{object_name}%')
          AND ah.QUERY_START_TIME >= DATEADD('day', -90, CURRENT_TIMESTAMP())
        GROUP BY 1, 2, 3
        ORDER BY ACCESS_COUNT DESC
        LIMIT {limit}
    """)
    return df


def get_columns_metadata(database: str, schema: str, object_name: str) -> pd.DataFrame:
    """Métadonnées des colonnes d'un objet (types, nullable, commentaire)."""
    df = run_sql(f"""
        SELECT
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            COMMENT
        FROM {database}.INFORMATION_SCHEMA.COLUMNS
        WHERE UPPER(TABLE_SCHEMA) = UPPER('{schema}')
          AND UPPER(TABLE_NAME)   = UPPER('{object_name}')
        ORDER BY ORDINAL_POSITION
    """)
    return df


# ══════════════════════════════════════════════════════════════════════════════
# LINEAGE HEURISTIQUE — QUERY_HISTORY
# ══════════════════════════════════════════════════════════════════════════════

def get_query_lineage_heuristic(
    object_name: str,
    days: int = 30,
    limit: int = 100,
) -> pd.DataFrame:
    """
    Heuristique : trouve les requêtes qui mentionnent cet objet dans
    QUERY_HISTORY et extrait les autres tables référencées.
    Confiance : UNKNOWN (pattern matching uniquement).
    """
    df = run_sql(f"""
        SELECT
            QUERY_ID,
            QUERY_TEXT,
            DATABASE_NAME,
            SCHEMA_NAME,
            USER_NAME,
            ROLE_NAME,
            QUERY_TYPE,
            START_TIME,
            TOTAL_ELAPSED_TIME / 1000 AS DURATION_SEC,
            ROWS_PRODUCED
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE UPPER(QUERY_TEXT) LIKE UPPER('%{object_name}%')
          AND QUERY_TYPE IN ('SELECT', 'INSERT', 'CREATE_TABLE_AS_SELECT',
                             'MERGE', 'UPDATE')
          AND START_TIME >= DATEADD('day', -{days}, CURRENT_TIMESTAMP())
          AND EXECUTION_STATUS = 'SUCCESS'
        ORDER BY START_TIME DESC
        LIMIT {limit}
    """)
    return df


def get_object_summary(database: str, schema: str, object_name: str) -> dict:
    """Résumé rapide d'un objet (type, row count, last altered, comment)."""
    df = run_sql(f"""
        SELECT
            TABLE_NAME,
            TABLE_TYPE,
            ROW_COUNT,
            BYTES,
            CREATED,
            LAST_ALTERED,
            COMMENT
        FROM {database}.INFORMATION_SCHEMA.TABLES
        WHERE UPPER(TABLE_SCHEMA) = UPPER('{schema}')
          AND UPPER(TABLE_NAME)   = UPPER('{object_name}')
        LIMIT 1
    """)
    if df.empty:
        return {}
    row = df.iloc[0].to_dict()
    return row
