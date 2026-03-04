"""
lineage_queries.py — Toutes les requêtes SQL pour le Data Lineage
Sources: ACCOUNT_USAGE.OBJECT_DEPENDENCIES, INFORMATION_SCHEMA, QUERY_HISTORY
"""
import pandas as pd
from modules.snowflake_client import run_sql, run_sql_no_cache


# ══════════════════════════════════════════════════════════════════════════════
# NAVIGATION : DB / Schema / Objects
# ══════════════════════════════════════════════════════════════════════════════

def get_databases() -> list[str]:
    """Liste toutes les bases de données accessibles."""
    df = run_sql("SHOW DATABASES")
    return sorted(df["name"].tolist()) if not df.empty else []


def get_schemas(database: str) -> list[str]:
    """Liste les schémas d'une base de données."""
    df = run_sql(f"SHOW SCHEMAS IN DATABASE {database}")
    if df.empty:
        return []
    # Exclure les schémas système
    exclude = {"INFORMATION_SCHEMA", "PUBLIC"}
    return sorted([s for s in df["name"].tolist() if s not in exclude])


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
# LINEAGE OBJET — ACCOUNT_USAGE.OBJECT_DEPENDENCIES
# ══════════════════════════════════════════════════════════════════════════════

def get_upstream_dependencies(
    database: str,
    schema: str,
    object_name: str,
    max_depth: int = 3,
) -> pd.DataFrame:
    """
    Lineage upstream : de quels objets dépend l'objet sélectionné ?
    Utilise OBJECT_DEPENDENCIES (Snowflake natif).
    Confiance : CERTAIN (source directe).
    """
    df = run_sql(f"""
        WITH RECURSIVE upstream AS (
            -- Niveau 0 : dépendances directes
            SELECT
                REFERENCED_DATABASE      AS SRC_DB,
                REFERENCED_SCHEMA        AS SRC_SCHEMA,
                REFERENCED_OBJECT_NAME   AS SRC_OBJECT,
                REFERENCED_OBJECT_DOMAIN AS SRC_TYPE,
                REFERENCING_DATABASE     AS TGT_DB,
                REFERENCING_SCHEMA       AS TGT_SCHEMA,
                REFERENCING_OBJECT_NAME  AS TGT_OBJECT,
                REFERENCING_OBJECT_DOMAIN AS TGT_TYPE,
                1 AS DEPTH,
                'CERTAIN' AS CONFIDENCE
            FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
            WHERE UPPER(REFERENCING_DATABASE)   = UPPER('{database}')
              AND UPPER(REFERENCING_SCHEMA)      = UPPER('{schema}')
              AND UPPER(REFERENCING_OBJECT_NAME) = UPPER('{object_name}')

            UNION ALL

            -- Niveaux suivants (récursion)
            SELECT
                d.REFERENCED_DATABASE,
                d.REFERENCED_SCHEMA,
                d.REFERENCED_OBJECT_NAME,
                d.REFERENCED_OBJECT_DOMAIN,
                d.REFERENCING_DATABASE,
                d.REFERENCING_SCHEMA,
                d.REFERENCING_OBJECT_NAME,
                d.REFERENCING_OBJECT_DOMAIN,
                u.DEPTH + 1,
                'CERTAIN'
            FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES d
            INNER JOIN upstream u
                ON UPPER(d.REFERENCING_DATABASE)   = UPPER(u.SRC_DB)
               AND UPPER(d.REFERENCING_SCHEMA)      = UPPER(u.SRC_SCHEMA)
               AND UPPER(d.REFERENCING_OBJECT_NAME) = UPPER(u.SRC_OBJECT)
            WHERE u.DEPTH < {max_depth}
        )
        SELECT DISTINCT * FROM upstream
        ORDER BY DEPTH, SRC_OBJECT
    """)
    return df


def get_downstream_dependencies(
    database: str,
    schema: str,
    object_name: str,
    max_depth: int = 3,
) -> pd.DataFrame:
    """
    Lineage downstream : quels objets dépendent de l'objet sélectionné ?
    """
    df = run_sql(f"""
        WITH RECURSIVE downstream AS (
            SELECT
                REFERENCED_DATABASE      AS SRC_DB,
                REFERENCED_SCHEMA        AS SRC_SCHEMA,
                REFERENCED_OBJECT_NAME   AS SRC_OBJECT,
                REFERENCED_OBJECT_DOMAIN AS SRC_TYPE,
                REFERENCING_DATABASE     AS TGT_DB,
                REFERENCING_SCHEMA       AS TGT_SCHEMA,
                REFERENCING_OBJECT_NAME  AS TGT_OBJECT,
                REFERENCING_OBJECT_DOMAIN AS TGT_TYPE,
                1 AS DEPTH,
                'CERTAIN' AS CONFIDENCE
            FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
            WHERE UPPER(REFERENCED_DATABASE)   = UPPER('{database}')
              AND UPPER(REFERENCED_SCHEMA)      = UPPER('{schema}')
              AND UPPER(REFERENCED_OBJECT_NAME) = UPPER('{object_name}')

            UNION ALL

            SELECT
                d.REFERENCED_DATABASE,
                d.REFERENCED_SCHEMA,
                d.REFERENCED_OBJECT_NAME,
                d.REFERENCED_OBJECT_DOMAIN,
                d.REFERENCING_DATABASE,
                d.REFERENCING_SCHEMA,
                d.REFERENCING_OBJECT_NAME,
                d.REFERENCING_OBJECT_DOMAIN,
                dn.DEPTH + 1,
                'CERTAIN'
            FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES d
            INNER JOIN downstream dn
                ON UPPER(d.REFERENCED_DATABASE)   = UPPER(dn.TGT_DB)
               AND UPPER(d.REFERENCED_SCHEMA)      = UPPER(dn.TGT_SCHEMA)
               AND UPPER(d.REFERENCED_OBJECT_NAME) = UPPER(dn.TGT_OBJECT)
            WHERE dn.DEPTH < {max_depth}
        )
        SELECT DISTINCT * FROM downstream
        ORDER BY DEPTH, TGT_OBJECT
    """)
    return df


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
