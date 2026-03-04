"""
graph_builder.py — Graphe de lineage via vis-network (CDN)
Zero dépendances externes : pure Python + json + pandas
Compatible Streamlit in Snowflake (aucun package à installer)
"""
import json
import pandas as pd

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
CENTER_STYLE = {"bg": "#0f3460", "border": "#00d4ff", "font": "#ffffff"}


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def _short_name(full_name: str) -> str:
    parts = full_name.split(".")
    name = parts[-1] if parts else full_name
    return name if len(name) <= 22 else name[:20] + "…"


def _full_name(row: pd.Series, prefix: str) -> str:
    db     = str(row.get(f"{prefix}_DB",     "") or "")
    schema = str(row.get(f"{prefix}_SCHEMA", "") or "")
    obj    = str(row.get(f"{prefix}_OBJECT", "") or "")
    parts  = [p for p in [db, schema, obj] if p]
    return ".".join(parts) if parts else obj


def _make_node(node_id: str, node_type: str, depth: int, is_center: bool) -> dict:
    if is_center:
        style = CENTER_STYLE
        shape = "diamond"
        size  = 36
    else:
        style = NODE_COLORS.get(node_type, NODE_COLORS["UNKNOWN"])
        shape = "ellipse"
        size  = max(16, 28 - depth * 4)

    return {
        "id":    node_id,
        "label": _short_name(node_id),
        "title": f"<b>{node_id}</b><br>Type: {node_type}<br>Profondeur: {depth}",
        "color": {
            "background": style["bg"],
            "border":     style["border"],
            "highlight":  {"background": style["border"], "border": "#ffffff"},
        },
        "font":        {"color": style["font"], "size": 12, "face": "DM Mono"},
        "shape":       shape,
        "size":        size,
        "borderWidth": 2,
    }


def _make_edge(src: str, tgt: str, conf: str, direction: str) -> dict:
    color  = EDGE_COLORS.get(conf, EDGE_COLORS["CERTAIN"])
    dashes = (conf != "CERTAIN")
    return {
        "from":   src,
        "to":     tgt,
        "color":  {"color": color, "highlight": "#ffffff"},
        "arrows": {"to": {"enabled": True, "scaleFactor": 0.8}},
        "dashes": dashes,
        "width":  2 if conf == "CERTAIN" else 1,
        "title":  f"Confiance: {conf} | Direction: {direction}",
        "smooth": {"type": "curvedCW", "roundness": 0.15},
    }


# ══════════════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════════════

def build_object_graph(
    upstream_df:   pd.DataFrame,
    downstream_df: pd.DataFrame,
    center_object: str,
    center_type:   str = "TABLE",
) -> str:
    """
    Construit le graphe lineage objet (upstream + downstream).
    Retourne du HTML autonome utilisant vis-network via CDN.
    Aucun package Python externe requis.
    """
    nodes: dict[str, dict] = {}
    edges: list[dict]      = []

    # Nœud central
    center_node = _make_node(center_object, center_type, 0, is_center=True)
    center_node["title"] = (
        f"<b>{center_object}</b><br>Type: {center_type}<br><i>Objet sélectionné</i>"
    )
    nodes[center_object] = center_node

    # Upstream
    for _, row in upstream_df.iterrows():
        src   = _full_name(row, "SRC")
        tgt   = _full_name(row, "TGT")
        conf  = str(row.get("CONFIDENCE", "CERTAIN")).upper()
        depth = int(row.get("DEPTH", 1))

        if src not in nodes:
            nodes[src] = _make_node(src, str(row.get("SRC_TYPE", "TABLE")).upper(),
                                    depth, src == center_object)
        if tgt not in nodes:
            nodes[tgt] = _make_node(tgt, str(row.get("TGT_TYPE", "TABLE")).upper(),
                                    depth, tgt == center_object)
        edges.append(_make_edge(src, tgt, conf, "upstream"))

    # Downstream
    for _, row in downstream_df.iterrows():
        src   = _full_name(row, "SRC")
        tgt   = _full_name(row, "TGT")
        conf  = str(row.get("CONFIDENCE", "CERTAIN")).upper()
        depth = int(row.get("DEPTH", 1))

        if src not in nodes:
            nodes[src] = _make_node(src, str(row.get("SRC_TYPE", "TABLE")).upper(),
                                    depth, src == center_object)
        if tgt not in nodes:
            nodes[tgt] = _make_node(tgt, str(row.get("TGT_TYPE", "TABLE")).upper(),
                                    depth, tgt == center_object)
        edges.append(_make_edge(src, tgt, conf, "downstream"))

    return _render_html(list(nodes.values()), edges, height=580)


def build_column_graph(columns_df: pd.DataFrame, object_name: str) -> str:
    """Graphe simplifié : table centrale → colonnes accédées."""
    nodes: dict[str, dict] = {}
    edges: list[dict]      = []

    # Nœud central
    nodes[object_name] = _make_node(object_name, "TABLE", 0, is_center=True)

    for _, row in columns_df.iterrows():
        col_name = str(row.get("COLUMN_NAME", "?"))
        obj_full = str(row.get("OBJECT_FULL_NAME", object_name))
        col_id   = f"{obj_full}.{col_name}"
        count    = int(row.get("ACCESS_COUNT", 1))
        obj_type = str(row.get("OBJECT_TYPE", "TABLE")).upper()

        if col_id not in nodes:
            n = _make_node(col_id, obj_type, 1, is_center=False)
            n["label"] = col_name
            n["title"] = (
                f"<b>{col_name}</b><br>Accès: {count}"
                f"<br>Dernière: {row.get('LAST_ACCESSED', '')}"
            )
            nodes[col_id] = n

        edges.append(_make_edge(object_name, col_id, "PROBABLE", "column"))

    return _render_html(list(nodes.values()), edges, height=420)


# ══════════════════════════════════════════════════════════════════════════════
# Rendu HTML (vis-network via CDN — zéro dépendance Python)
# ══════════════════════════════════════════════════════════════════════════════

def _render_html(nodes_data: list, edges_data: list, height: int = 580) -> str:
    nodes_json = json.dumps(nodes_data)
    edges_json = json.dumps(edges_data)

    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ background:#0a0e1a; }}
  #graph {{ width:100%; height:{height}px; background:#0d1224; }}
  .legend {{
    position:absolute; top:12px; right:12px;
    background:rgba(13,18,36,0.94); border:1px solid #1e3a5f;
    border-radius:8px; padding:10px 14px;
    font:11px 'DM Mono',monospace; color:#94a3b8;
  }}
  .li {{ display:flex; align-items:center; gap:8px; margin:4px 0; }}
  .dot {{ width:10px; height:10px; border-radius:2px; border:1.5px solid; }}
  .controls {{
    position:absolute; bottom:12px; left:12px; display:flex; gap:6px;
  }}
  .btn {{
    background:#111827; border:1px solid #1e3a5f; color:#64748b;
    border-radius:6px; padding:5px 10px; cursor:pointer;
    font:10px 'DM Mono',monospace;
  }}
  .btn:hover {{ border-color:#38bdf8; color:#38bdf8; }}
</style>
<script src="https://cdnjs.cloudflare.com/ajax/libs/vis-network/9.1.9/standalone/umd/vis-network.min.js"></script>
</head>
<body style="position:relative">
<div id="graph"></div>

<div class="legend">
  <div style="color:#e2e8f0;font-weight:600;margin-bottom:8px">LÉGENDE</div>
  <div class="li"><div class="dot" style="background:#1e3a5f;border-color:#38bdf8"></div>TABLE</div>
  <div class="li"><div class="dot" style="background:#1e2d55;border-color:#818cf8"></div>VIEW</div>
  <div class="li"><div class="dot" style="background:#1a3a2a;border-color:#34d399"></div>STREAM</div>
  <div style="border-top:1px solid #1e3a5f;margin:8px 0"></div>
  <div class="li"><div style="width:20px;height:2px;background:#38bdf8"></div>Certain</div>
  <div class="li"><div style="width:20px;border-top:1px dashed #fbbf24"></div>Probable</div>
</div>

<div class="controls">
  <button class="btn" onclick="network.fit()">⊡ Fit</button>
  <button class="btn" onclick="network.setOptions({{physics:{{enabled:true}}}})">⟳ Reset</button>
</div>

<script>
  var nodes = new vis.DataSet({nodes_json});
  var edges = new vis.DataSet({edges_json});
  var net   = new vis.Network(
    document.getElementById('graph'),
    {{ nodes, edges }},
    {{
      physics: {{
        enabled: true,
        stabilization: {{ iterations: 150 }},
        barnesHut: {{ gravitationalConstant:-6000, springLength:120, damping:0.09 }}
      }},
      interaction: {{ hover:true, tooltipDelay:150, zoomView:true, dragView:true }},
      nodes: {{ borderWidth:2, shadow:{{ enabled:true, color:'rgba(0,100,255,0.3)', size:12 }} }},
      edges: {{ width:2, selectionWidth:3 }}
    }}
  );
  net.once('stabilizationIterationsDone', () =>
    net.fit({{ animation:{{ duration:800, easingFunction:'easeInOutQuad' }} }})
  );
  net.on('click', p => {{
    if (p.nodes.length)
      net.focus(p.nodes[0], {{ scale:1.2, animation:{{ duration:500 }} }});
  }});
</script>
</body>
</html>"""
