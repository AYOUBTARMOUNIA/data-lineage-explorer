"""
graph_builder.py — Construction du graphe de lineage avec NetworkX + Pyvis
Rendu interactif via st.components.v1.html (compatible Streamlit in Snowflake)
"""
import json
import textwrap
import pandas as pd
import networkx as nx

# ── Palette ──────────────────────────────────────────────────────────────────
NODE_COLORS = {
    "TABLE":   {"bg": "#1e3a5f", "border": "#38bdf8", "font": "#e2e8f0"},
    "VIEW":    {"bg": "#1e2d55", "border": "#818cf8", "font": "#e2e8f0"},
    "STREAM":  {"bg": "#1a3a2a", "border": "#34d399", "font": "#e2e8f0"},
    "TASK":    {"bg": "#3a1e1e", "border": "#f87171", "font": "#e2e8f0"},
    "UNKNOWN": {"bg": "#2a1e3a", "border": "#a78bfa", "font": "#e2e8f0"},
}

EDGE_COLORS = {
    "CERTAIN":  "#38bdf8",
    "PROBABLE": "#fbbf24",
    "UNKNOWN":  "#a78bfa",
}

CENTER_NODE_STYLE = {
    "bg": "#0f3460",
    "border": "#00d4ff",
    "font": "#ffffff",
    "size": 36,
}


# ══════════════════════════════════════════════════════════════════════════════
# Builders
# ══════════════════════════════════════════════════════════════════════════════

def build_object_graph(
    upstream_df: pd.DataFrame,
    downstream_df: pd.DataFrame,
    center_object: str,
    center_type: str = "TABLE",
) -> str:
    """
    Construit le graphe de lineage objet et retourne le HTML Pyvis.

    Parameters
    ----------
    upstream_df   : DataFrame avec colonnes SRC_OBJECT, SRC_TYPE, TGT_OBJECT, TGT_TYPE, DEPTH, CONFIDENCE
    downstream_df : idem
    center_object : nom de l'objet central
    center_type   : type de l'objet central (TABLE / VIEW / ...)

    Returns
    -------
    str : HTML complet du graphe interactif (à injecter via st.components.v1.html)
    """
    G = nx.DiGraph()

    # ── Nœud central ──────────────────────────────────────────────────────────
    G.add_node(
        center_object,
        label=_short_name(center_object),
        title=f"<b>{center_object}</b><br>Type: {center_type}<br><i>Objet sélectionné</i>",
        group="CENTER",
        node_type=center_type,
        depth=0,
        is_center=True,
    )

    # ── Upstream ──────────────────────────────────────────────────────────────
    for _, row in upstream_df.iterrows():
        src = _full_name(row, "SRC")
        tgt = _full_name(row, "TGT")
        src_type = str(row.get("SRC_TYPE", "TABLE")).upper()
        tgt_type = str(row.get("TGT_TYPE", "TABLE")).upper()
        conf = str(row.get("CONFIDENCE", "CERTAIN")).upper()
        depth = int(row.get("DEPTH", 1))

        _add_node(G, src, src_type, depth, is_center=(src == center_object))
        _add_node(G, tgt, tgt_type, depth, is_center=(tgt == center_object))

        G.add_edge(src, tgt, direction="upstream", confidence=conf, depth=depth)

    # ── Downstream ────────────────────────────────────────────────────────────
    for _, row in downstream_df.iterrows():
        src = _full_name(row, "SRC")
        tgt = _full_name(row, "TGT")
        src_type = str(row.get("SRC_TYPE", "TABLE")).upper()
        tgt_type = str(row.get("TGT_TYPE", "TABLE")).upper()
        conf = str(row.get("CONFIDENCE", "CERTAIN")).upper()
        depth = int(row.get("DEPTH", 1))

        _add_node(G, src, src_type, depth, is_center=(src == center_object))
        _add_node(G, tgt, tgt_type, depth, is_center=(tgt == center_object))

        G.add_edge(src, tgt, direction="downstream", confidence=conf, depth=depth)

    # ── Rendu HTML ────────────────────────────────────────────────────────────
    return _render_html(G, center_object)


def build_column_graph(
    columns_df: pd.DataFrame,
    object_name: str,
) -> str:
    """
    Graphe simplifié pour le lineage colonne.
    Affiche les colonnes accédées avec leur fréquence.
    """
    G = nx.DiGraph()

    # Nœud table centrale
    G.add_node(object_name, label=_short_name(object_name), group="CENTER", is_center=True)

    for _, row in columns_df.iterrows():
        col_node = f"{row.get('OBJECT_FULL_NAME', object_name)}.{row.get('COLUMN_NAME', '?')}"
        col_label = str(row.get("COLUMN_NAME", "?"))
        count = int(row.get("ACCESS_COUNT", 1))
        obj_type = str(row.get("OBJECT_TYPE", "TABLE")).upper()

        G.add_node(
            col_node,
            label=col_label,
            title=f"<b>{col_label}</b><br>Accès: {count}<br>Dernière: {row.get('LAST_ACCESSED', '')}",
            group=obj_type,
            is_center=False,
            access_count=count,
        )
        G.add_edge(object_name, col_node, direction="column", confidence="PROBABLE")

    return _render_html(G, object_name, height=400)


# ══════════════════════════════════════════════════════════════════════════════
# Helpers privés
# ══════════════════════════════════════════════════════════════════════════════

def _full_name(row: pd.Series, prefix: str) -> str:
    """Construit le nom complet DB.SCHEMA.OBJECT depuis les colonnes du DataFrame."""
    db = row.get(f"{prefix}_DB", "")
    schema = row.get(f"{prefix}_SCHEMA", "")
    obj = row.get(f"{prefix}_OBJECT", "")
    parts = [p for p in [db, schema, obj] if p]
    return ".".join(parts) if parts else obj


def _short_name(full_name: str) -> str:
    """Retourne la dernière partie du nom (pour le label du nœud)."""
    parts = full_name.split(".")
    name = parts[-1] if parts else full_name
    # Tronquer si trop long
    return name if len(name) <= 20 else name[:18] + "…"


def _add_node(G: nx.DiGraph, name: str, node_type: str, depth: int, is_center: bool = False):
    if name in G.nodes:
        return
    short = _short_name(name)
    tooltip = f"<b>{name}</b><br>Type: {node_type}<br>Profondeur: {depth}"
    G.add_node(
        name,
        label=short,
        title=tooltip,
        group=node_type,
        node_type=node_type,
        depth=depth,
        is_center=is_center,
    )


def _render_html(G: nx.DiGraph, center_node: str, height: int = 580) -> str:
    """
    Génère le HTML complet du graphe Pyvis embarqué.
    On construit le HTML manuellement pour éviter les dépendances sur
    les fichiers statiques de Pyvis (non disponibles dans SiS).
    """
    nodes_data = []
    edges_data = []

    for node_id, attrs in G.nodes(data=True):
        is_center = attrs.get("is_center", False)
        group = attrs.get("group", "TABLE")

        if is_center and group == "CENTER":
            style = CENTER_NODE_STYLE
            shape = "diamond"
            size = 36
        else:
            style = NODE_COLORS.get(group, NODE_COLORS["UNKNOWN"])
            shape = "ellipse"
            size = max(18, 28 - attrs.get("depth", 1) * 4)

        nodes_data.append({
            "id": node_id,
            "label": attrs.get("label", _short_name(node_id)),
            "title": attrs.get("title", node_id),
            "color": {
                "background": style["bg"],
                "border": style["border"],
                "highlight": {"background": style["border"], "border": "#ffffff"},
            },
            "font": {"color": style.get("font", "#e2e8f0"), "size": 12, "face": "DM Mono"},
            "shape": shape,
            "size": size,
            "borderWidth": 2,
        })

    for src, tgt, attrs in G.edges(data=True):
        conf = attrs.get("confidence", "CERTAIN").upper()
        direction = attrs.get("direction", "upstream")
        color = EDGE_COLORS.get(conf, EDGE_COLORS["CERTAIN"])
        dashes = conf != "CERTAIN"

        edges_data.append({
            "from": src,
            "to": tgt,
            "color": {"color": color, "highlight": "#ffffff"},
            "arrows": {"to": {"enabled": True, "scaleFactor": 0.8}},
            "dashes": dashes,
            "width": 2 if conf == "CERTAIN" else 1,
            "title": f"Confiance: {conf} | Direction: {direction}",
            "smooth": {"type": "curvedCW", "roundness": 0.15},
        })

    nodes_json = json.dumps(nodes_data)
    edges_json = json.dumps(edges_data)

    html = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{ background: #0a0e1a; font-family: 'DM Mono', monospace; }}
    #graph {{ width: 100%; height: {height}px; background: #0d1224; border-radius: 0; }}

    .legend {{
      position: absolute; top: 12px; right: 12px;
      background: rgba(13,18,36,0.92);
      border: 1px solid #1e3a5f;
      border-radius: 8px;
      padding: 10px 14px;
      font-size: 11px;
      color: #94a3b8;
      font-family: 'DM Mono', monospace;
    }}
    .legend-item {{ display: flex; align-items: center; gap: 8px; margin: 4px 0; }}
    .legend-dot {{
      width: 10px; height: 10px; border-radius: 2px;
      border: 1.5px solid currentColor;
    }}
    .controls {{
      position: absolute; bottom: 12px; left: 12px;
      display: flex; gap: 6px;
    }}
    .ctrl-btn {{
      background: #111827; border: 1px solid #1e3a5f;
      color: #64748b; border-radius: 6px;
      padding: 5px 10px; cursor: pointer;
      font-family: 'DM Mono', monospace; font-size: 10px;
    }}
    .ctrl-btn:hover {{ border-color: #38bdf8; color: #38bdf8; }}
  </style>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/vis-network/9.1.9/standalone/umd/vis-network.min.js"></script>
</head>
<body style="position:relative">
  <div id="graph"></div>

  <!-- Légende -->
  <div class="legend">
    <div style="color:#e2e8f0;font-weight:600;margin-bottom:8px;font-size:11px">LÉGENDE</div>
    <div class="legend-item">
      <div class="legend-dot" style="background:#1e3a5f;border-color:#38bdf8;color:#38bdf8"></div>
      <span>TABLE</span>
    </div>
    <div class="legend-item">
      <div class="legend-dot" style="background:#1e2d55;border-color:#818cf8;color:#818cf8"></div>
      <span>VIEW</span>
    </div>
    <div class="legend-item">
      <div class="legend-dot" style="background:#1a3a2a;border-color:#34d399;color:#34d399"></div>
      <span>STREAM</span>
    </div>
    <div style="border-top:1px solid #1e3a5f;margin:8px 0"></div>
    <div class="legend-item">
      <div style="width:20px;height:2px;background:#38bdf8;margin:0 4px"></div>
      <span>Certain</span>
    </div>
    <div class="legend-item">
      <div style="width:20px;height:1px;border-top:1px dashed #fbbf24;margin:0 4px"></div>
      <span>Probable</span>
    </div>
  </div>

  <!-- Contrôles -->
  <div class="controls">
    <button class="ctrl-btn" onclick="network.fit()">⊡ Fit</button>
    <button class="ctrl-btn" onclick="network.setOptions({{physics:{{enabled:true}}}})">⟳ Reset</button>
  </div>

  <script>
    var nodes = new vis.DataSet({nodes_json});
    var edges = new vis.DataSet({edges_json});

    var container = document.getElementById('graph');
    var data = {{ nodes: nodes, edges: edges }};
    var options = {{
      layout: {{
        improvedLayout: true,
        hierarchical: {{
          enabled: false
        }}
      }},
      physics: {{
        enabled: true,
        stabilization: {{ iterations: 150, updateInterval: 25 }},
        barnesHut: {{
          gravitationalConstant: -6000,
          centralGravity: 0.3,
          springLength: 120,
          springConstant: 0.04,
          damping: 0.09
        }}
      }},
      interaction: {{
        hover: true,
        tooltipDelay: 150,
        navigationButtons: false,
        keyboard: true,
        zoomView: true,
        dragView: true
      }},
      nodes: {{
        borderWidth: 2,
        shadow: {{ enabled: true, color: 'rgba(0,100,255,0.3)', size: 12, x: 0, y: 4 }}
      }},
      edges: {{
        width: 2,
        selectionWidth: 3,
        hoverWidth: 3
      }}
    }};

    var network = new vis.Network(container, data, options);

    // Centrer sur le nœud central après stabilisation
    network.once('stabilizationIterationsDone', function() {{
      network.fit({{ animation: {{ duration: 800, easingFunction: 'easeInOutQuad' }} }});
    }});

    // Highlight on click
    network.on('click', function(params) {{
      if (params.nodes.length > 0) {{
        network.focus(params.nodes[0], {{
          scale: 1.2,
          animation: {{ duration: 500, easingFunction: 'easeInOutQuad' }}
        }});
      }}
    }});
  </script>
</body>
</html>
"""
    return html
