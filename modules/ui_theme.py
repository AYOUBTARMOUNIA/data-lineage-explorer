"""
ui_theme.py — Dark premium CSS theme for Data Lineage Explorer
Inspired by the PIH HTML design (Syne / DM Mono fonts, dark palette)
"""
import streamlit as st


DARK_CSS = """
<style>
  @import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=DM+Mono:wght@300;400;500&display=swap');

  /* ── Global ── */
  html, body, [class*="css"] {
      font-family: 'Syne', sans-serif;
      background-color: #0a0e1a;
      color: #e2e8f0;
  }
  .stApp { background-color: #0a0e1a; }

  /* ── Sidebar ── */
  section[data-testid="stSidebar"] {
      background: linear-gradient(180deg, #0d1224 0%, #0a0e1a 100%);
      border-right: 1px solid #1e2d4a;
  }
  section[data-testid="stSidebar"] * { font-family: 'Syne', sans-serif; }

  /* ── Metric cards ── */
  [data-testid="metric-container"] {
      background: #111827;
      border: 1px solid #1e3a5f;
      border-radius: 12px;
      padding: 16px 20px;
      box-shadow: 0 4px 24px rgba(0,100,255,0.06);
  }
  [data-testid="metric-container"] label {
      color: #64748b !important;
      font-family: 'DM Mono', monospace !important;
      font-size: 11px !important;
      letter-spacing: 0.08em;
      text-transform: uppercase;
  }
  [data-testid="metric-container"] [data-testid="stMetricValue"] {
      color: #38bdf8 !important;
      font-size: 26px !important;
      font-weight: 700;
  }

  /* ── Select boxes ── */
  .stSelectbox > div > div {
      background: #111827 !important;
      border: 1px solid #1e3a5f !important;
      border-radius: 8px !important;
      color: #e2e8f0 !important;
  }

  /* ── Tabs ── */
  .stTabs [data-baseweb="tab-list"] {
      background: #111827;
      border-radius: 10px;
      padding: 4px;
      gap: 4px;
  }
  .stTabs [data-baseweb="tab"] {
      background: transparent;
      color: #64748b;
      border-radius: 8px;
      font-family: 'DM Mono', monospace;
      font-size: 12px;
      letter-spacing: 0.05em;
      padding: 8px 18px;
  }
  .stTabs [aria-selected="true"] {
      background: #1e3a5f !important;
      color: #38bdf8 !important;
  }

  /* ── Dataframe ── */
  [data-testid="stDataFrame"] {
      border: 1px solid #1e3a5f;
      border-radius: 10px;
      overflow: hidden;
  }

  /* ── Expander ── */
  .streamlit-expanderHeader {
      background: #111827 !important;
      border: 1px solid #1e3a5f !important;
      border-radius: 8px !important;
      color: #94a3b8 !important;
      font-family: 'DM Mono', monospace !important;
      font-size: 12px !important;
  }

  /* ── Slider ── */
  .stSlider [data-baseweb="slider"] { padding: 0 4px; }
  .stSlider [data-testid="stTickBarMin"],
  .stSlider [data-testid="stTickBarMax"] {
      color: #64748b;
      font-family: 'DM Mono', monospace;
      font-size: 11px;
  }

  /* ── Badges / chips ── */
  .badge {
      display: inline-block;
      padding: 2px 10px;
      border-radius: 20px;
      font-family: 'DM Mono', monospace;
      font-size: 11px;
      font-weight: 500;
      letter-spacing: 0.04em;
  }
  .badge-table    { background: #1e3a5f; color: #38bdf8; }
  .badge-view     { background: #1e3355; color: #818cf8; }
  .badge-stream   { background: #1e3322; color: #34d399; }
  .badge-task     { background: #3a1e1e; color: #f87171; }
  .badge-certain  { background: #1e3322; color: #34d399; }
  .badge-probable { background: #3a2e1e; color: #fbbf24; }
  .badge-unknown  { background: #2a1e3a; color: #a78bfa; }

  /* ── Graph container ── */
  .graph-container {
      background: #0d1224;
      border: 1px solid #1e3a5f;
      border-radius: 12px;
      overflow: hidden;
  }

  /* ── Headers ── */
  h1, h2, h3 {
      font-family: 'Syne', sans-serif !important;
      font-weight: 700;
  }
  h1 { font-size: 28px; color: #f1f5f9; letter-spacing: -0.02em; }
  h2 { font-size: 20px; color: #cbd5e1; }
  h3 { font-size: 16px; color: #94a3b8; }

  /* ── Info / warning boxes ── */
  .info-box {
      background: #0f1e38;
      border-left: 3px solid #38bdf8;
      border-radius: 0 8px 8px 0;
      padding: 12px 16px;
      margin: 8px 0;
      font-family: 'DM Mono', monospace;
      font-size: 12px;
      color: #94a3b8;
  }
  .warn-box {
      background: #1a1200;
      border-left: 3px solid #fbbf24;
      border-radius: 0 8px 8px 0;
      padding: 12px 16px;
      margin: 8px 0;
      font-family: 'DM Mono', monospace;
      font-size: 12px;
      color: #fcd34d;
  }

  /* ── Scrollbar ── */
  ::-webkit-scrollbar { width: 6px; height: 6px; }
  ::-webkit-scrollbar-track { background: #0a0e1a; }
  ::-webkit-scrollbar-thumb { background: #1e3a5f; border-radius: 3px; }
  ::-webkit-scrollbar-thumb:hover { background: #38bdf8; }
</style>
"""


def apply_theme():
    """Injecter le CSS dark premium dans Streamlit."""
    st.markdown(DARK_CSS, unsafe_allow_html=True)


def badge(label: str, kind: str = "table") -> str:
    """Retourner un badge HTML coloré selon le type d'objet."""
    kind_map = {
        "TABLE": "badge-table",
        "VIEW": "badge-view",
        "STREAM": "badge-stream",
        "TASK": "badge-task",
        "CERTAIN": "badge-certain",
        "PROBABLE": "badge-probable",
        "UNKNOWN": "badge-unknown",
    }
    css_class = kind_map.get(kind.upper(), "badge-table")
    return f'<span class="badge {css_class}">{label}</span>'


def info_box(text: str):
    st.markdown(f'<div class="info-box">{text}</div>', unsafe_allow_html=True)


def warn_box(text: str):
    st.markdown(f'<div class="warn-box">⚠️ {text}</div>', unsafe_allow_html=True)
