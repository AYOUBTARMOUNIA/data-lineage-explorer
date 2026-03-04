"""
Data Lineage Explorer — Streamlit in Snowflake
================================================
Visualisation interactive du lineage objet + colonne
Sources : OBJECT_DEPENDENCIES · INFORMATION_SCHEMA · QUERY_HISTORY
"""
import streamlit as st
import pandas as pd

from modules.ui_theme import apply_theme, badge, info_box, warn_box
from modules.snowflake_client import log_action
from modules.lineage_queries import (
    get_databases,
    get_schemas,
    get_objects,
    get_upstream_dependencies,
    get_downstream_dependencies,
    get_column_lineage,
    get_columns_metadata,
    get_query_lineage_heuristic,
    get_object_summary,
)
from modules.graph_builder import build_object_graph, build_column_graph


# ── Helper utilitaire (doit être défini avant tout appel) ─────────────────────
def _fmt_bytes(b) -> str:
    if not b:
        return "—"
    try:
        b = float(b)
    except (TypeError, ValueError):
        return "—"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} PB"


# ── Setup page ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Data Lineage Explorer",
    page_icon="🔗",
    layout="wide",
    initial_sidebar_state="expanded",
)
apply_theme()


# ══════════════════════════════════════════════════════════════════════════════
# HEADER
# ══════════════════════════════════════════════════════════════════════════════
col_title, col_badge = st.columns([6, 1])
with col_title:
    st.markdown("""
    <h1 style="margin-bottom:4px">
      🔗 Data Lineage Explorer
    </h1>
    <p style="color:#64748b;font-family:'DM Mono',monospace;font-size:12px;margin:0">
      Visualisation upstream / downstream · Objet &amp; Colonne · Snowflake native
    </p>
    """, unsafe_allow_html=True)
with col_badge:
    st.markdown('<div style="text-align:right;padding-top:12px">'
                + badge("READ-ONLY", "CERTAIN")
                + "</div>", unsafe_allow_html=True)

st.markdown("---")


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR — Sélecteurs
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("### ⚙️ Objet à analyser")

    # Chargement DB
    with st.spinner("Chargement des bases…"):
        try:
            db_list = get_databases()
        except Exception as e:
            st.error(f"Erreur accès databases : {e}")
            db_list = []

    if not db_list:
        st.warning("Aucune base de données accessible.")
        st.stop()

    selected_db = st.selectbox("🗄️ Base de données", db_list)

    # Chargement Schémas
    with st.spinner("Chargement des schémas…"):
        try:
            schema_list = get_schemas(selected_db)
        except Exception as e:
            st.error(f"Erreur accès schémas : {e}")
            schema_list = []

    if not schema_list:
        st.info("Aucun schéma trouvé (hors INFORMATION_SCHEMA).")
        st.stop()

    selected_schema = st.selectbox("📂 Schéma", schema_list)

    # Chargement Objets
    with st.spinner("Chargement des objets…"):
        try:
            objects_df = get_objects(selected_db, selected_schema)
        except Exception as e:
            st.error(f"Erreur chargement objets : {e}")
            objects_df = pd.DataFrame()

    if objects_df.empty:
        st.info("Aucun objet dans ce schéma.")
        st.stop()

    # Filtre par type
    type_filter = st.multiselect(
        "Type d'objet",
        options=["BASE TABLE", "VIEW"],
        default=["BASE TABLE", "VIEW"],
    )
    filtered_objects = objects_df[objects_df["OBJECT_TYPE"].isin(type_filter)]
    obj_names = filtered_objects["OBJECT_NAME"].tolist()

    if not obj_names:
        st.info("Aucun objet pour ce filtre.")
        st.stop()

    # Recherche dans la liste
    search_obj = st.text_input("🔍 Chercher un objet", placeholder="nom partiel…")
    if search_obj:
        obj_names = [o for o in obj_names if search_obj.upper() in o.upper()]
        if not obj_names:
            st.warning("Aucun objet ne correspond.")
            st.stop()

    selected_object = st.selectbox("📦 Objet", obj_names)

    st.markdown("---")
    st.markdown("### 📐 Paramètres graphe")

    max_depth = st.slider("Profondeur (hops)", min_value=1, max_value=5, value=3)

    direction = st.radio(
        "Direction",
        options=["⬆️ Upstream", "⬇️ Downstream", "↕️ Les deux"],
        index=2,
        horizontal=True,
    )

    show_columns = st.toggle("Afficher le lineage colonne", value=True)
    show_query_history = st.toggle("Heuristique (Query History)", value=False)

    st.markdown("---")
    run_btn = st.button("🔍 Analyser le lineage", type="primary", use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# ÉTAT DE SESSION — mémoriser le dernier objet analysé
# ══════════════════════════════════════════════════════════════════════════════
if "last_analyzed" not in st.session_state:
    st.session_state.last_analyzed = None
if "lineage_data" not in st.session_state:
    st.session_state.lineage_data = {}

if run_btn:
    st.session_state.last_analyzed = {
        "db": selected_db,
        "schema": selected_schema,
        "object": selected_object,
        "depth": max_depth,
        "direction": direction,
    }
    st.session_state.lineage_data = {}  # reset cache local
    log_action(
        module="lineage",
        action="ANALYZE",
        object_name=f"{selected_db}.{selected_schema}.{selected_object}",
        details=f"depth={max_depth} direction={direction}",
    )


# ══════════════════════════════════════════════════════════════════════════════
# ZONE PRINCIPALE
# ══════════════════════════════════════════════════════════════════════════════
if not st.session_state.last_analyzed:
    st.markdown("""
    <div style="text-align:center;padding:80px 0;color:#1e3a5f">
      <div style="font-size:64px;margin-bottom:16px">🔗</div>
      <div style="font-size:18px;color:#64748b;font-family:'Syne',sans-serif;font-weight:600">
        Sélectionnez un objet et cliquez sur <b style="color:#38bdf8">Analyser</b>
      </div>
      <div style="font-size:12px;color:#334155;font-family:'DM Mono',monospace;margin-top:8px">
        Base → Schéma → Objet → Profondeur → Direction
      </div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()


# ── Récupération des données ──────────────────────────────────────────────────
ctx = st.session_state.last_analyzed
db, schema, obj = ctx["db"], ctx["schema"], ctx["object"]
depth = ctx["depth"]
dir_ = ctx["direction"]

full_name = f"{db}.{schema}.{obj}"

# Métadonnées objet
with st.spinner(f"Chargement des métadonnées de {obj}…"):
    try:
        obj_meta = get_object_summary(db, schema, obj)
    except Exception as e:
        warn_box(f"Métadonnées indisponibles : {e}")
        obj_meta = {}

# ── Barre d'info objet ────────────────────────────────────────────────────────
obj_type = obj_meta.get("TABLE_TYPE", "TABLE")
type_badge = "VIEW" if "VIEW" in str(obj_type).upper() else "TABLE"

st.markdown(
    f"""
    <div style="
      background:#111827;border:1px solid #1e3a5f;border-radius:10px;
      padding:16px 20px;margin-bottom:20px;display:flex;align-items:center;gap:16px
    ">
      <div>
        <div style="font-size:20px;font-weight:700;color:#f1f5f9">{obj}</div>
        <div style="font-size:11px;font-family:'DM Mono',monospace;color:#64748b;margin-top:4px">
          {db} › {schema}
        </div>
      </div>
      <div style="margin-left:auto;display:flex;gap:10px;align-items:center">
        {badge(type_badge, type_badge)}
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# ── Métriques rapides ─────────────────────────────────────────────────────────
m1, m2, m3, m4 = st.columns(4)
row_count = obj_meta.get("ROW_COUNT")
size_bytes = obj_meta.get("BYTES", 0) or 0
last_alt = str(obj_meta.get("LAST_ALTERED", "—"))[:10]
comment = obj_meta.get("COMMENT") or "—"

m1.metric("Nb lignes", f"{int(row_count):,}" if row_count else "—")
m2.metric("Taille", _fmt_bytes(size_bytes))
m3.metric("Dernière modif.", last_alt)
m4.metric("Commentaire", comment[:30] + "…" if len(str(comment)) > 30 else comment)


# (_fmt_bytes défini en haut du fichier)


# ══════════════════════════════════════════════════════════════════════════════
# TABS
# ══════════════════════════════════════════════════════════════════════════════
tab_graph, tab_table, tab_cols, tab_history = st.tabs([
    "🌐 Graphe interactif",
    "📋 Liste des dépendances",
    "🔬 Lineage colonne",
    "📜 Query History",
])


# ── TAB 1 : Graphe interactif ─────────────────────────────────────────────────
with tab_graph:
    with st.spinner("Construction du graphe…"):
        upstream_df   = pd.DataFrame()
        downstream_df = pd.DataFrame()
        all_errors: list[str] = []

        if "⬆️" in dir_ or "↕️" in dir_:
            upstream_df, errs = get_upstream_dependencies(db, schema, obj, depth)
            all_errors.extend(errs)

        if "⬇️" in dir_ or "↕️" in dir_:
            downstream_df, errs = get_downstream_dependencies(db, schema, obj, depth)
            all_errors.extend(errs)

    # Stats rapides
    n_up   = len(upstream_df)   if not upstream_df.empty   else 0
    n_down = len(downstream_df) if not downstream_df.empty else 0

    sc1, sc2, sc3 = st.columns(3)
    sc1.metric("⬆️ Dépendances upstream",   n_up)
    sc2.metric("⬇️ Dépendances downstream", n_down)
    sc3.metric("Total edges", n_up + n_down)

    # ── Diagnostic des erreurs ────────────────────────────────────────────────
    if all_errors:
        with st.expander("🔍 Diagnostic — erreurs de requêtes (cliquer pour voir)", expanded=(n_up + n_down == 0)):
            for err in all_errors:
                st.markdown(
                    f'<div style="background:#1a0f0f;border-left:3px solid #f87171;'
                    f'border-radius:0 6px 6px 0;padding:8px 12px;margin:4px 0;'
                    f'font-family:DM Mono,monospace;font-size:11px;color:#fca5a5">'
                    f'{err}</div>',
                    unsafe_allow_html=True,
                )
            st.markdown(
                '<div style="margin-top:10px;font-size:11px;color:#64748b;font-family:DM Mono,monospace">'
                '💡 <b>Solutions :</b><br>'
                '• <b>GET_LINEAGE</b> : nécessite Snowflake Enterprise + SNOWFLAKE.CORE accessible<br>'
                '• <b>OBJECT_DEPENDENCIES</b> : exécuter <code>GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE &lt;ton_role&gt;</code><br>'
                '• Si ni l\'un ni l\'autre : activer <b>Heuristique Query History</b> dans la sidebar'
                '</div>',
                unsafe_allow_html=True,
            )

    if n_up + n_down == 0:
        st.info("Aucune dépendance trouvée. Consultez le diagnostic ci-dessus.")
    else:
        graph_html = build_object_graph(
            upstream_df=upstream_df,
            downstream_df=downstream_df,
            center_object=full_name,
            center_type=type_badge,
        )
        st.markdown('<div class="graph-container">', unsafe_allow_html=True)
        st.components.v1.html(graph_html, height=600, scrolling=False)
        st.markdown('</div>', unsafe_allow_html=True)

        src_label = "GET_LINEAGE" if not all_errors else "OBJECT_DEPENDENCIES"
        st.markdown(
            f"""<div style="display:flex;gap:12px;margin-top:8px;font-family:'DM Mono',monospace;font-size:11px;color:#64748b">
              <span>Trait plein = {badge('CERTAIN','CERTAIN')}</span>
              <span>Trait pointillé = {badge('PROBABLE','PROBABLE')}</span>
              <span>⚙️ Source: {src_label}</span>
            </div>""",
            unsafe_allow_html=True,
        )


# ── TAB 2 : Liste tabulaire ───────────────────────────────────────────────────
with tab_table:
    frames = []
    if not upstream_df.empty:
        up = upstream_df.copy()
        up["DIRECTION"] = "⬆️ Upstream"
        frames.append(up)
    if not downstream_df.empty:
        dn = downstream_df.copy()
        dn["DIRECTION"] = "⬇️ Downstream"
        frames.append(dn)

    if not frames:
        info_box("Lancez l'analyse (onglet Graphe) pour afficher les dépendances.")
    else:
        combined = pd.concat(frames, ignore_index=True)

        # Filtres
        col_dir, col_conf, col_type = st.columns(3)
        with col_dir:
            dir_filter = st.multiselect("Direction", ["⬆️ Upstream", "⬇️ Downstream"],
                                        default=["⬆️ Upstream", "⬇️ Downstream"])
        with col_conf:
            conf_filter = st.multiselect("Confiance", ["CERTAIN", "PROBABLE", "UNKNOWN"],
                                         default=["CERTAIN", "PROBABLE", "UNKNOWN"])
        with col_type:
            type_opts = combined["SRC_TYPE"].dropna().unique().tolist()
            type_flt = st.multiselect("Type source", type_opts, default=type_opts)

        mask = (
            combined["DIRECTION"].isin(dir_filter) &
            combined["CONFIDENCE"].isin(conf_filter) &
            combined["SRC_TYPE"].isin(type_flt)
        )
        display = combined[mask][[
            "DIRECTION", "SRC_DB", "SRC_SCHEMA", "SRC_OBJECT", "SRC_TYPE",
            "TGT_DB", "TGT_SCHEMA", "TGT_OBJECT", "TGT_TYPE",
            "DEPTH", "CONFIDENCE"
        ]].rename(columns={
            "SRC_DB": "DB Source", "SRC_SCHEMA": "Schema Source",
            "SRC_OBJECT": "Objet Source", "SRC_TYPE": "Type Source",
            "TGT_DB": "DB Cible", "TGT_SCHEMA": "Schema Cible",
            "TGT_OBJECT": "Objet Cible", "TGT_TYPE": "Type Cible",
            "DEPTH": "Profondeur", "CONFIDENCE": "Confiance",
        })

        st.caption(f"{len(display)} dépendance(s) affichée(s)")
        st.dataframe(display, use_container_width=True, hide_index=True)

        # Export
        csv = display.to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇️ Exporter CSV",
            data=csv,
            file_name=f"lineage_{obj}.csv",
            mime="text/csv",
        )


# ── TAB 3 : Lineage colonne ───────────────────────────────────────────────────
with tab_cols:
    if not show_columns:
        info_box("Activez 'Lineage colonne' dans la sidebar pour voir cet onglet.")
    else:
        sub_col, sub_meta = st.columns([1, 1])

        # Colonnes ACCESS_HISTORY
        with sub_col:
            st.markdown("#### 📊 Colonnes accédées (ACCESS_HISTORY)")
            with st.spinner("Chargement lineage colonne…"):
                try:
                    col_lineage_df = get_column_lineage(db, schema, obj)
                except Exception as e:
                    warn_box(f"ACCESS_HISTORY indisponible : {e}")
                    col_lineage_df = pd.DataFrame()

            if col_lineage_df.empty:
                info_box(
                    "Aucune donnée dans ACCESS_HISTORY pour cet objet (90 derniers jours). "
                    "Vérifiez les droits ACCOUNT_USAGE."
                )
            else:
                st.dataframe(
                    col_lineage_df[["COLUMN_NAME", "OBJECT_TYPE", "ACCESS_COUNT",
                                    "LAST_ACCESSED", "CONFIDENCE"]],
                    use_container_width=True,
                    hide_index=True,
                )
                # Mini graphe colonne
                if len(col_lineage_df) <= 50:
                    col_html = build_column_graph(col_lineage_df, full_name)
                    st.components.v1.html(col_html, height=420, scrolling=False)

        # Metadata colonnes
        with sub_meta:
            st.markdown("#### 🗂️ Structure des colonnes (INFORMATION_SCHEMA)")
            with st.spinner("Chargement métadonnées colonnes…"):
                try:
                    meta_df = get_columns_metadata(db, schema, obj)
                except Exception as e:
                    warn_box(f"Erreur : {e}")
                    meta_df = pd.DataFrame()

            if meta_df.empty:
                info_box("Aucune colonne trouvée dans INFORMATION_SCHEMA.")
            else:
                st.dataframe(meta_df, use_container_width=True, hide_index=True)
                st.caption(f"{len(meta_df)} colonne(s)")


# ── TAB 4 : Query History heuristique ────────────────────────────────────────
with tab_history:
    if not show_query_history:
        info_box("Activez 'Heuristique (Query History)' dans la sidebar.")
    else:
        days_qh = st.slider("Période (jours)", 7, 90, 30, key="qh_days")

        with st.spinner("Analyse de l'historique de requêtes…"):
            try:
                qh_df = get_query_lineage_heuristic(obj, days=days_qh)
            except Exception as e:
                warn_box(f"QUERY_HISTORY indisponible : {e}")
                qh_df = pd.DataFrame()

        if qh_df.empty:
            info_box(f"Aucune requête trouvant '{obj}' dans les {days_qh} derniers jours.")
        else:
            st.caption(
                f"{len(qh_df)} requête(s) référençant **{obj}** · "
                f"Confiance : {badge('UNKNOWN', 'UNKNOWN')}",
                unsafe_allow_html=True,
            )

            # Résumé par type
            if "QUERY_TYPE" in qh_df.columns:
                by_type = qh_df.groupby("QUERY_TYPE").size().reset_index(name="COUNT")
                st.bar_chart(by_type.set_index("QUERY_TYPE")["COUNT"])

            # Table
            display_cols = [c for c in [
                "QUERY_TYPE", "USER_NAME", "ROLE_NAME", "START_TIME",
                "DURATION_SEC", "ROWS_PRODUCED", "DATABASE_NAME", "SCHEMA_NAME"
            ] if c in qh_df.columns]

            st.dataframe(qh_df[display_cols], use_container_width=True, hide_index=True)

            with st.expander("🔍 Voir les textes de requêtes"):
                for i, row in qh_df.head(10).iterrows():
                    st.markdown(
                        f"""<div style="background:#111827;border:1px solid #1e3a5f;
                          border-radius:8px;padding:12px;margin:6px 0">
                          <div style="font-family:'DM Mono',monospace;font-size:10px;
                            color:#64748b;margin-bottom:6px">
                            {row.get('START_TIME','')} · {row.get('USER_NAME','')}
                            · {row.get('QUERY_TYPE','')}
                          </div>
                          <code style="font-size:11px;color:#94a3b8;white-space:pre-wrap">
                            {str(row.get('QUERY_TEXT',''))[:500]}
                          </code>
                        </div>""",
                        unsafe_allow_html=True,
                    )
