# 🔗 Data Lineage Explorer — Streamlit in Snowflake

Application native **Streamlit in Snowflake** pour visualiser le lineage de données (objet + colonne) directement depuis Snowsight.

## Fonctionnalités

| Module | Description |
|--------|-------------|
| 🌐 Graphe interactif | Upstream / downstream via Pyvis + vis-network |
| 📋 Liste dépendances | Table filtrables + export CSV |
| 🔬 Lineage colonne | ACCESS_HISTORY + structure INFORMATION_SCHEMA |
| 📜 Query History | Heuristique pattern-matching sur QUERY_HISTORY |

## Sources de données
- `SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES` — lineage natif (CERTAIN)
- `SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY` — lineage colonne (PROBABLE)
- `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` — heuristique (UNKNOWN)
- `{DB}.INFORMATION_SCHEMA.*` — métadonnées objets/colonnes

## Déploiement (Streamlit in Snowflake)

1. **Setup SQL** : exécuter `sql/setup.sql` avec un rôle SYSADMIN
2. **Snowsight** → Streamlit → New App
3. Coller le contenu de `app.py` dans l'éditeur
4. Ajouter les packages dans l'onglet Packages : `networkx`, `pyvis`
5. Cliquer **Run** ▶️

## Sécurité
- **READ-ONLY** : aucune DDL/DML possible depuis l'app
- Audit log automatique dans `DATA_HUB.APP_CONFIG.APP_AUDIT_LOG`
- RBAC Snowflake respecté nativement (session = utilisateur connecté)

## Structure du projet
```
data-lineage-app/
├── app.py                      # App principale (multi-tabs)
├── requirements.txt            # Packages Snowflake Anaconda
├── modules/
│   ├── __init__.py
│   ├── snowflake_client.py     # Session + SQL helper + audit
│   ├── lineage_queries.py      # Toutes les requêtes SQL
│   ├── graph_builder.py        # NetworkX + Pyvis → HTML
│   └── ui_theme.py             # CSS dark premium
└── sql/
    └── setup.sql               # Tables + droits à créer une fois
```
