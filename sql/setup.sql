-- ============================================================
--  Setup SQL — Data Lineage Explorer
--  À exécuter une seule fois avec un rôle SYSADMIN / GOV_ADMIN
-- ============================================================

-- Base et schéma dédiés à l'app
CREATE DATABASE IF NOT EXISTS DATA_HUB;
CREATE SCHEMA  IF NOT EXISTS DATA_HUB.APP_CONFIG;

-- Table d'audit applicatif
CREATE TABLE IF NOT EXISTS DATA_HUB.APP_CONFIG.APP_AUDIT_LOG (
    LOG_ID            NUMBER AUTOINCREMENT PRIMARY KEY,
    EVENT_TIMESTAMP   TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    MODULE            VARCHAR(100),
    ACTION            VARCHAR(200),
    OBJECT_NAME       VARCHAR(500),
    DETAILS           VARCHAR(2000),
    SESSION_USER      VARCHAR(200) DEFAULT CURRENT_USER()
);

-- Index implicite via clustering
ALTER TABLE DATA_HUB.APP_CONFIG.APP_AUDIT_LOG
    CLUSTER BY (EVENT_TIMESTAMP, MODULE);

-- Droits minimaux pour l'app (rôle lecteur)
-- Remplacer DATA_LINEAGE_READER par votre rôle applicatif
GRANT USAGE ON DATABASE DATA_HUB TO ROLE DATA_LINEAGE_READER;
GRANT USAGE ON SCHEMA DATA_HUB.APP_CONFIG TO ROLE DATA_LINEAGE_READER;
GRANT SELECT ON TABLE DATA_HUB.APP_CONFIG.APP_AUDIT_LOG TO ROLE DATA_LINEAGE_READER;
GRANT INSERT ON TABLE DATA_HUB.APP_CONFIG.APP_AUDIT_LOG TO ROLE DATA_LINEAGE_READER;

-- Accès ACCOUNT_USAGE (requis pour OBJECT_DEPENDENCIES + ACCESS_HISTORY)
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE DATA_LINEAGE_READER;

-- ============================================================
-- Vérification
-- ============================================================
SELECT COUNT(*) AS NB_LOGS FROM DATA_HUB.APP_CONFIG.APP_AUDIT_LOG;
