import html as _html
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
import requests
from datetime import datetime
from urllib.parse import quote

from modules.data_generator import get_historical_data, CAUSE_CATEGORY, CATEGORY_COLOR
from modules.scraper import (
    fetch_all_realtime,
    fetch_all_history_with_causes,
    fetch_tohoku,
    fetch_tohoku_detail_df,
    WEATHER_FLAG_CONFIG,
)

# ─── ページ設定 ────────────────────────────────────────────────
st.set_page_config(
    page_title="全国停電情報ダッシュボード",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─── CSS ──────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+JP:wght@400;500;700&display=swap');
html, body, [class*="css"] {
    font-family: 'Noto Sans JP', sans-serif;
    color: #1e293b;
}

/* ── 明るい背景・テキスト強制 ──────────────── */
.stApp { background-color: #eef3fb; color: #1e293b; }
.stApp p, .stApp span, .stApp div, .stApp label,
.stApp li, .stApp h1, .stApp h2, .stApp h3,
[data-testid="stMarkdownContainer"] { color: #1e293b; }
section[data-testid="stSidebar"] { background: #dce8f7; }
.block-container { background-color: transparent; }

/* ── ヘッダー（明るいブルー） ────────────── */
.main-header {
    background: linear-gradient(135deg, #1d6ae5 0%, #3b82f6 55%, #60a5fa 100%);
    padding: 20px 28px; border-radius: 14px;
    margin-bottom: 20px; color: white;
    display: flex; align-items: center; justify-content: space-between;
    box-shadow: 0 4px 20px rgba(59,130,246,0.35);
}
.main-header h1 { font-size: 1.6rem; font-weight: 700; margin: 0; }
.main-header p  { font-size: 0.82rem; opacity: 0.85; margin: 3px 0 0; }
.header-right   { text-align: right; font-size: 0.8rem; opacity: 0.9; }

/* ── KPI カード ──────────────────────────── */
.kpi-card {
    background: white; border-radius: 12px;
    padding: 14px 18px; border-left: 5px solid;
    box-shadow: 0 2px 12px rgba(0,0,0,0.07);
}
.kpi-card.red    { border-left-color: #ef4444; }
.kpi-card.orange { border-left-color: #f97316; }
.kpi-card.blue   { border-left-color: #3b82f6; }
.kpi-card.green  { border-left-color: #22c55e; }
.kpi-card.gray   { border-left-color: #9ca3af; }
.kpi-card.indigo { border-left-color: #6366f1; }
.kpi-label { font-size: 0.7rem; color: #6b7280; font-weight: 600;
             text-transform: uppercase; letter-spacing: 0.05em; }
.kpi-value { font-size: 1.8rem; font-weight: 700; color: #111827; line-height: 1.2; }
.kpi-sub   { font-size: 0.75rem; color: #9ca3af; margin-top: 3px; }

/* ── セクションタイトル ─────────────────── */
.section-title {
    font-size: 1rem; font-weight: 700; color: #1e3a8a;
    border-left: 4px solid #3b82f6; padding-left: 10px;
    margin: 18px 0 10px; background: white;
    border-radius: 0 8px 8px 0; padding: 7px 12px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
}

/* ── 凡例バー ────────────────────────────── */
.legend-bar {
    display: flex; gap: 16px; align-items: center;
    background: white; border-radius: 8px; padding: 10px 16px;
    margin-bottom: 10px; font-size: 0.8rem; flex-wrap: wrap;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
}
.legend-item { display: flex; align-items: center; gap: 6px; }
.dot { width: 14px; height: 14px; border-radius: 50%; display: inline-block; flex-shrink: 0; }

/* ── カバレッジバー ──────────────────────── */
.coverage-bar {
    background: white; border-radius: 10px; padding: 10px 16px;
    border: 1px solid #dbeafe; margin-bottom: 12px;
    display: flex; align-items: center; gap: 12px; flex-wrap: wrap; font-size: 0.8rem;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
}
.coverage-tag {
    display: inline-flex; align-items: center; gap: 4px;
    border-radius: 20px; padding: 3px 10px; font-size: 0.72rem; font-weight: 600;
}
.tag-ok   { background: #dcfce7; color: #166534; }
.tag-ng   { background: #e0e7ff; color: #3730a3; }

/* ── 都道府県リスト ──────────────────────── */
.pref-scroll {
    height: 520px; overflow-y: auto;
    border: 1px solid #dbeafe; border-radius: 10px; background: white;
    box-shadow: 0 2px 8px rgba(0,0,0,0.06);
}
.pref-item {
    display: flex; align-items: center; gap: 10px;
    padding: 10px 14px; border-bottom: 1px solid #f0f4ff;
}
.pref-item:last-child { border-bottom: none; }
.pref-dot  { width: 12px; height: 12px; border-radius: 50%; flex-shrink: 0; }
.pref-name { font-weight: 600; font-size: 0.88rem; flex: 1; }
.pref-count { font-size: 0.9rem; font-weight: 700; color: #111827; min-width: 80px; text-align: right; }
.pref-meta  { font-size: 0.68rem; color: #9ca3af; }

/* ── 事故起因カード ──────────────────────── */
.cause-card {
    background: white; border-radius: 10px; padding: 12px 14px;
    margin-bottom: 8px; border: 1px solid #dbeafe;
    display: flex; align-items: center; gap: 12px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.05);
}
.cause-badge {
    border-radius: 6px; padding: 3px 10px; font-size: 0.72rem;
    font-weight: 700; color: white; white-space: nowrap;
}
.cause-name  { font-size: 0.88rem; font-weight: 600; flex: 1; }
.cause-count { font-size: 1.1rem; font-weight: 700; color: #111827; }
.cause-sub   { font-size: 0.72rem; color: #9ca3af; }

/* ── 各社詳細カード ──────────────────────── */
.company-info-bar {
    background: white; border-radius: 10px; padding: 10px 16px;
    border-left: 5px solid #3b82f6; margin-bottom: 14px;
    font-size: 0.82rem; color: #374151;
    box-shadow: 0 1px 6px rgba(0,0,0,0.07);
}
.pref-card {
    background: white; border-radius: 10px; padding: 12px 10px;
    text-align: center; border: 1.5px solid;
    box-shadow: 0 2px 8px rgba(0,0,0,0.07);
}
</style>
""", unsafe_allow_html=True)

# ─── 色定義（停電レベル → 色）─────────────────────────────────
LEVEL_COLORS = {
    "停電なし":          "#4ade80",   # 緑
    "〜1,000軒":         "#fbbf24",   # 黄
    "1,001〜10,000軒":   "#f97316",   # 橙
    "10,001軒以上":      "#dc2626",   # 赤
    "データ未取得":      "#cbd5e1",   # グレー
}

# ─── GeoJSON キャッシュ ────────────────────────────────────────
@st.cache_data(ttl=86400, show_spinner=False)
def load_japan_geojson():
    try:
        url = "https://raw.githubusercontent.com/dataofjapan/land/master/japan.geojson"
        resp = requests.get(url, timeout=12)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


# ─── マップ作成（離散カテゴリーによる着色）────────────────────
def build_choropleth(df: pd.DataFrame, geojson) -> go.Figure:
    """停電レベル別に離散色分けしたコロプレスマップ"""
    fig = px.choropleth_mapbox(
        df,
        geojson=geojson,
        locations="code",
        featureidkey="id",
        color="outage_level",
        color_discrete_map=LEVEL_COLORS,
        category_orders={"outage_level": list(LEVEL_COLORS.keys())},
        hover_name="prefecture",
        hover_data={
            "code": False,
            "outage_level": True,
            "affected_customers": ":,",
            "data_status": True,
            "data_source": True,
            "fetched_at": True,
        },
        labels={
            "outage_level": "停電規模",
            "affected_customers": "停電軒数",
            "data_status": "データ状態",
            "data_source": "情報元",
            "fetched_at": "情報更新",
        },
        mapbox_style="carto-positron",
        center={"lat": 36.5, "lon": 137.5},
        zoom=4.2,
        opacity=0.85,
    )
    fig.update_layout(
        height=540,
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        legend=dict(
            title="停電規模",
            orientation="v",
            x=0.01, y=0.99,
            bgcolor="rgba(255,255,255,0.9)",
            bordercolor="#e5e7eb",
            borderwidth=1,
            font=dict(size=11),
        ),
    )
    return fig


def build_bubble_map(df: pd.DataFrame) -> go.Figure:
    """GeoJSON取得不可時のバブルマップ（フォールバック）"""
    fig = go.Figure()
    for level, color in LEVEL_COLORS.items():
        sub = df[df["outage_level"] == level]
        if sub.empty:
            continue
        size = sub["affected_customers"].apply(lambda x: max(8, np.sqrt(x) / 4 + 8) if x > 0 else 8)
        fig.add_trace(go.Scattermapbox(
            lat=sub["lat"], lon=sub["lon"], mode="markers",
            marker=go.scattermapbox.Marker(size=size, color=color, opacity=0.85),
            text=sub.apply(
                lambda r: f"<b>{r['prefecture']}</b><br>"
                          f"停電軒数: {r['affected_customers']:,}軒<br>"
                          f"規模: {r['outage_level']}<br>"
                          f"情報元: {r['data_source']}",
                axis=1,
            ),
            hoverinfo="text", name=level,
        ))
    fig.update_layout(
        mapbox_style="carto-positron",
        mapbox=dict(center={"lat": 36.5, "lon": 137.5}, zoom=4.0),
        height=540, margin={"r": 0, "t": 0, "l": 0, "b": 0},
        legend=dict(title="停電規模"),
    )
    return fig


def pref_list_html(df: pd.DataFrame) -> str:
    """Yahoo風の都道府県リスト HTML"""
    active  = df[df["affected_customers"] > 0].sort_values("affected_customers", ascending=False)
    no_out  = df[(df["affected_customers"] == 0) & (df["data_status"] == "取得済み")]
    no_data = df[df["data_status"] == "取得不可"]

    items = ""
    if not active.empty:
        items += "<div style='padding:8px 14px; font-size:0.7rem; color:#ef4444; font-weight:700; background:#fef2f2;'>● 停電中</div>"
        for _, r in active.iterrows():
            color = LEVEL_COLORS.get(r["outage_level"], "#ccc")
            items += f"""<div class="pref-item">
              <div class="pref-dot" style="background:{color}"></div>
              <div>
                <div class="pref-name">{r['prefecture']}</div>
                <div class="pref-meta">{r['data_source']}</div>
              </div>
              <div style="text-align:right">
                <div class="pref-count">{r['affected_customers']:,}軒</div>
                <div class="pref-meta">{r['outage_level']}</div>
              </div>
            </div>"""

    if not no_out.empty:
        items += "<div style='padding:8px 14px; font-size:0.7rem; color:#16a34a; font-weight:700; background:#f0fdf4;'>● 停電なし（確認済み）</div>"
        for _, r in no_out.sort_values("prefecture").iterrows():
            items += f"""<div class="pref-item">
              <div class="pref-dot" style="background:#4ade80"></div>
              <div>
                <div class="pref-name">{r['prefecture']}</div>
                <div class="pref-meta">{r['data_source']}</div>
              </div>
              <div style="text-align:right">
                <div class="pref-count" style="color:#16a34a">0軒</div>
                <div class="pref-meta">{r['fetched_at'][:16]}</div>
              </div>
            </div>"""

    if not no_data.empty:
        items += "<div style='padding:8px 14px; font-size:0.7rem; color:#64748b; font-weight:700; background:#f8fafc;'>○ データ未取得</div>"
        for _, r in no_data.sort_values("prefecture").iterrows():
            items += f"""<div class="pref-item">
              <div class="pref-dot" style="background:#cbd5e1"></div>
              <div>
                <div class="pref-name">{r['prefecture']}</div>
                <div class="pref-meta">{r['data_source']}</div>
              </div>
              <div style="text-align:right; color:#9ca3af">
                <div class="pref-count" style="font-size:0.8rem">—</div>
              </div>
            </div>"""

    if not items:
        items = "<div style='padding:30px; text-align:center; color:#9ca3af;'>データがありません</div>"
    return f'<div class="pref-scroll">{items}</div>'


def coverage_html(df: pd.DataFrame) -> str:
    ok_sources = df[df["data_status"] == "取得済み"]["data_source"].unique()
    ng_sources = df[df["data_status"] == "取得不可"]["data_source"].unique()
    ok_tags = "".join(
        f'<span class="coverage-tag tag-ok">✓ {s}</span>' for s in sorted(set(ok_sources))
    )
    ng_tags = "".join(
        f'<span class="coverage-tag tag-ng">✕ {s}</span>' for s in sorted(set(ng_sources))
    )
    ok_count = (df["data_status"] == "取得済み").sum()
    return (
        f'<div class="coverage-bar">'
        f'<b style="font-size:0.78rem">データカバレッジ: {ok_count}/47 都道府県</b>'
        f'{ok_tags}{ng_tags}'
        f'</div>'
    )


# ─── ヘッダー ─────────────────────────────────────────────────
now_str = datetime.now().strftime("%Y年%m月%d日 %H:%M")
col_h1, col_h2 = st.columns([3, 1])
with col_h1:
    st.markdown(f"""
    <div class="main-header">
      <div>
        <h1>⚡ 全国停電情報ダッシュボード</h1>
        <p>各電力ネットワーク会社ホームページから取得したリアルタイム停電情報を可視化</p>
      </div>
      <div class="header-right">表示更新<br><b>{now_str}</b></div>
    </div>
    """, unsafe_allow_html=True)
with col_h2:
    auto_refresh = st.toggle("60秒ごとに自動更新", value=False)
    if st.button("🔄 今すぐ更新", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    if auto_refresh:
        st.markdown('<meta http-equiv="refresh" content="60">', unsafe_allow_html=True)

# ─── 履歴データキャッシュ ─────────────────────────────────────
def make_gmaps_url(pref: str, area: str) -> str:
    """都道府県名 + 地域名からGoogle Maps検索URLを生成する。
    area_name の最初の ，（全角コンマ）前までをクエリに使用。"""
    # "仙台市青葉区　上愛子，愛子中央４丁目，…" → "仙台市青葉区　上愛子"
    location = area.split("\uff0c")[0].strip() if area else ""
    query = f"日本 {pref} {location}".strip()
    return f"https://www.google.com/maps/search/?api=1&query={quote(query)}"


def build_outage_table_html(df: pd.DataFrame) -> str:
    """停電記録 DataFrame を、停電地域がクリッカブルリンクの HTML テーブルに変換する。"""
    _TH = (
        "padding:7px 10px; text-align:left; font-size:0.7rem; font-weight:700;"
        " color:#6b7280; text-transform:uppercase; letter-spacing:.04em;"
        " background:#f8fafc; border-bottom:2px solid #e5e7eb;"
        " white-space:nowrap; position:sticky; top:0; z-index:1;"
    )
    _TD = "padding:7px 10px; border-bottom:1px solid #f3f4f6; font-size:0.8rem; vertical-align:top;"

    headers = [
        ("起因",                "text-align:center; white-space:nowrap; min-width:120px;"),
        ("発生日",              "white-space:nowrap;"),
        ("都道府県",            "white-space:nowrap;"),
        ("停電地域  ※クリック → Google Maps", "min-width:220px;"),
        ("起因（原文）",        "min-width:160px;"),
        ("停電軒数",            "text-align:right; white-space:nowrap;"),
        ("発生時刻",            "white-space:nowrap;"),
        ("復旧時刻",            "white-space:nowrap;"),
        ("停電時間",            "text-align:right; white-space:nowrap;"),
    ]
    head_html = "".join(
        f'<th style="{_TH}{s}">{_html.escape(h)}</th>' for h, s in headers
    )

    rows_html = ""
    for i, row in df.iterrows():
        pref     = str(row.get("都道府県", ""))
        area     = str(row.get("停電地域", ""))
        gmaps    = make_gmaps_url(pref, area)
        dur_val  = row.get("停電時間(h)")
        dur_str  = f"{float(dur_val):.2f} h" if pd.notna(dur_val) and dur_val else "—"
        cnt_val  = row.get("停電軒数", 0)
        cnt_str  = f"{int(cnt_val):,} 軒" if pd.notna(cnt_val) else "—"
        wflag_raw = str(row.get("起因フラグ", row.get("天候影響", "不明")))
        bg        = "#fafafa" if i % 2 == 0 else "white"

        def td(val: str, extra: str = "") -> str:
            return f'<td style="{_TD}{extra}">{_html.escape(str(val))}</td>'

        # 複数フラグ（"|"区切り）を個別バッジとして描画
        flag_badges = ""
        for flag in wflag_raw.split("|"):
            flag = flag.strip()
            wcfg = WEATHER_FLAG_CONFIG.get(flag, WEATHER_FLAG_CONFIG["不明"])
            flag_badges += (
                f'<span style="display:inline-block; background:{wcfg["bg"]};'
                f' color:{wcfg["color"]}; border-radius:12px; padding:2px 8px;'
                f' font-size:0.7rem; font-weight:700; margin:1px 2px;">'
                f'{_html.escape(wcfg["label"])}</span>'
            )
        weather_cell = (
            f'<td style="{_TD}text-align:center; white-space:nowrap;">'
            f'{flag_badges}</td>'
        )
        area_cell = (
            f'<td style="{_TD}min-width:220px;">'
            f'<a href="{gmaps}" target="_blank" rel="noopener noreferrer"'
            f' title="Google Mapsで開く"'
            f' style="color:#2563eb; text-decoration:none; font-weight:500;"'
            f' onmouseover="this.style.textDecoration=\'underline\'"'
            f' onmouseout="this.style.textDecoration=\'none\'">'
            f'{_html.escape(area)}</a></td>'
        )

        rows_html += (
            f'<tr style="background:{bg};">'
            + weather_cell
            + td(str(row.get("発生日", "")),       "white-space:nowrap;")
            + td(pref,                              "white-space:nowrap;")
            + area_cell
            + td(str(row.get("起因（原文）", "")), "min-width:160px;")
            + f'<td style="{_TD}text-align:right; white-space:nowrap;">{cnt_str}</td>'
            + td(str(row.get("発生時刻", "")),     "white-space:nowrap;")
            + td(str(row.get("復旧時刻", "")),     "white-space:nowrap;")
            + f'<td style="{_TD}text-align:right; white-space:nowrap;">{dur_str}</td>'
            + "</tr>"
        )

    return (
        '<div style="overflow:auto; max-height:480px;'
        ' border:1px solid #e5e7eb; border-radius:8px; background:white;">'
        '<table style="width:100%; border-collapse:collapse;">'
        f"<thead><tr>{head_html}</tr></thead>"
        f"<tbody>{rows_html}</tbody>"
        "</table></div>"
    )


_CACHE_VERSION = "v3"   # weather_flag 列追加後にインクリメント → キャッシュ自動無効化

@st.cache_data(ttl=3600, show_spinner=False)
def load_history_data(_ver: str = _CACHE_VERSION):
    return fetch_all_history_with_causes()


@st.cache_data(ttl=60, show_spinner=False)
def load_realtime_data():
    return fetch_all_realtime()


@st.cache_data(ttl=300, show_spinner=False)
def load_tohoku_detail():
    return fetch_tohoku_detail_df()


@st.cache_data(ttl=60, show_spinner=False)
def load_tohoku_realtime():
    counts, ts = fetch_tohoku()
    return counts, ts


# ─── 各社 都道府県順・配色 ─────────────────────────────────────
_TOHOKU_PREF_ORDER = ["青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県", "新潟県"]
_TOHOKU_PREF_COLOR = {
    "青森県": "#3b82f6", "岩手県": "#06b6d4", "宮城県": "#10b981",
    "秋田県": "#f59e0b", "山形県": "#f97316", "福島県": "#ef4444", "新潟県": "#8b5cf6",
}

_RIKUDEN_PREF_ORDER = ["富山県", "石川県", "福井県"]
_RIKUDEN_PREF_COLOR = {"富山県": "#0284c7", "石川県": "#0891b2", "福井県": "#06b6d4"}

_CHUGOKU_PREF_ORDER = ["鳥取県", "島根県", "岡山県", "広島県", "山口県"]
_CHUGOKU_PREF_COLOR = {
    "鳥取県": "#7c3aed", "島根県": "#8b5cf6", "岡山県": "#a78bfa",
    "広島県": "#c084fc", "山口県": "#e879f9",
}

_KYUSHU_PREF_ORDER = ["福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県"]
_KYUSHU_PREF_COLOR = {
    "福岡県": "#dc2626", "佐賀県": "#ea580c", "長崎県": "#d97706",
    "熊本県": "#65a30d", "大分県": "#0891b2", "宮崎県": "#7c3aed", "鹿児島県": "#db2777",
}

_HOKKAIDO_PREF_ORDER = ["北海道"]
_HOKKAIDO_PREF_COLOR = {"北海道": "#1d4ed8"}

_KANSAI_PREF_ORDER = ["滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県"]
_KANSAI_PREF_COLOR = {
    "滋賀県": "#0369a1", "京都府": "#dc2626", "大阪府": "#ea580c",
    "兵庫県": "#65a30d", "奈良県": "#0891b2", "和歌山県": "#8b5cf6",
}

_SHIKOKU_PREF_ORDER = ["香川県", "愛媛県", "徳島県", "高知県"]
_SHIKOKU_PREF_COLOR = {
    "香川県": "#0369a1", "愛媛県": "#dc2626", "徳島県": "#65a30d", "高知県": "#d97706",
}

_TEPCO_PREF_ORDER = ["茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県", "山梨県", "静岡県"]
_TEPCO_PREF_COLOR = {
    "茨城県": "#3b82f6", "栃木県": "#06b6d4", "群馬県": "#10b981",
    "埼玉県": "#f59e0b", "千葉県": "#f97316", "東京都": "#ef4444",
    "神奈川県": "#8b5cf6", "山梨県": "#db2777", "静岡県": "#0891b2",
}

_CHUBU_PREF_ORDER = ["愛知県", "三重県", "岐阜県", "静岡県", "長野県"]
_CHUBU_PREF_COLOR = {
    "愛知県": "#0369a1", "三重県": "#0891b2", "岐阜県": "#10b981",
    "静岡県": "#f59e0b", "長野県": "#8b5cf6",
}

_OKINAWA_PREF_ORDER = ["沖縄県"]
_OKINAWA_PREF_COLOR = {"沖縄県": "#059669"}


# ─── 各社共通 詳細ビュー ───────────────────────────────────────
def _pref_cards(pref_order: list[str], pref_colors: dict[str, str],
                df_rt: pd.DataFrame, key_prefix: str):
    """都道府県別 リアルタイムカードを描画する"""
    n = len(pref_order)
    cols = st.columns(min(n, 7))
    for i, pref in enumerate(pref_order):
        row = df_rt[df_rt["prefecture"] == pref]
        if row.empty:
            count, status = None, "取得不可"
        else:
            r = row.iloc[0]
            count = r["affected_customers"] if r["data_status"] == "取得済み" else None
            status = r["data_status"]
        if count is None:
            bg, txt_c, val_str, bdc = "#f8fafc", "#64748b", "取得不可", "#e2e8f0"
        elif count == 0:
            bg, txt_c, val_str, bdc = "#f0fdf4", "#16a34a", "0 軒", "#bbf7d0"
        elif count <= 1000:
            bg, txt_c, val_str, bdc = "#fefce8", "#ca8a04", f"{count:,} 軒", "#fde68a"
        elif count <= 10000:
            bg, txt_c, val_str, bdc = "#fff7ed", "#c2410c", f"{count:,} 軒", "#fed7aa"
        else:
            bg, txt_c, val_str, bdc = "#fef2f2", "#b91c1c", f"{count:,} 軒", "#fecaca"
        dot_c = pref_colors.get(pref, "#6b7280")
        with cols[i % len(cols)]:
            st.markdown(
                f'<div class="pref-card" style="background:{bg}; border-color:{bdc};">'
                f'<div style="width:10px;height:10px;border-radius:50%;background:{dot_c};'
                f' margin:0 auto 5px;"></div>'
                f'<div style="font-size:0.78rem; font-weight:700; color:#374151;">{pref}</div>'
                f'<div style="font-size:1rem; font-weight:700; color:{txt_c};'
                f' margin-top:5px;">{val_str}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )


def _weather_summary_bar(dfc: pd.DataFrame) -> None:
    """起因フラグ サマリーバーを描画する"""
    from collections import Counter as _Counter
    _all_flags: list[str] = []
    for _fs in dfc["weather_flag"].fillna("不明"):
        _all_flags.extend(_fs.split("|"))
    wf_counts = _Counter(_all_flags)
    total_cnt = len(dfc)
    w_tags = ""
    for flag, cfg in WEATHER_FLAG_CONFIG.items():
        n   = wf_counts.get(flag, 0)
        pct = f"{n/total_cnt*100:.0f}%" if total_cnt > 0 else "—"
        w_tags += (
            f'<span style="display:inline-flex; align-items:center; gap:6px;'
            f' background:{cfg["bg"]}; color:{cfg["color"]}; border-radius:20px;'
            f' padding:4px 14px; font-size:0.78rem; font-weight:700;'
            f' margin-right:8px;">'
            f'{cfg["label"]} <span style="font-size:1rem;">{n}件</span>'
            f' <span style="opacity:.7;">({pct})</span></span>'
        )
    st.markdown(
        f'<div style="background:white; border:1px solid #dbeafe;'
        f' border-radius:8px; padding:10px 14px; margin:10px 0 4px;">'
        f'<span style="font-size:0.75rem; font-weight:700; color:#374151;'
        f' margin-right:12px;">起因フラグ判定</span>{w_tags}</div>',
        unsafe_allow_html=True,
    )
    return wf_counts, total_cnt


def render_company_detail(
    company_name: str,
    pref_order: list[str],
    pref_colors: dict[str, str],
    rt_url: str,
    hist_url: str,
    df_rt: pd.DataFrame,
    df_hist: pd.DataFrame,
    key_prefix: str,
    n_hist_days: str = "過去7日",
):
    """任意の電力会社の詳細ビューをレンダリングする汎用関数"""
    sub_rt, sub_hist = st.tabs(["📡 リアルタイム状況", "📅 履歴分析"])

    # ── サブタブ A: リアルタイム ──────────────────────────────
    with sub_rt:
        comp_rt = df_rt[df_rt["prefecture"].isin(pref_order)]
        ts_vals = comp_rt["fetched_at"].dropna()
        ts_str = ts_vals.iloc[0] if not ts_vals.empty else "—"

        st.markdown(
            f'<div class="company-info-bar">'
            f'情報更新: <b>{ts_str[:16] if ts_str != "—" else "—"}</b>'
            f'&ensp;|&ensp;<a href="{rt_url}" target="_blank" style="color:#2563eb;">'
            f'{company_name} 停電情報ページ</a></div>',
            unsafe_allow_html=True,
        )

        active_prefs = comp_rt[comp_rt["affected_customers"] > 0]
        total_count  = int(comp_rt["affected_customers"].sum())
        n_pref       = len(pref_order)
        max_row      = comp_rt.loc[comp_rt["affected_customers"].idxmax()] \
                       if not comp_rt.empty else None

        rt1, rt2, rt3 = st.columns(3)
        with rt1:
            st.markdown(f"""
            <div class="kpi-card red">
              <div class="kpi-label">停電中県数</div>
              <div class="kpi-value">{len(active_prefs)}</div>
              <div class="kpi-sub">/ {n_pref} 県（管内）</div>
            </div>""", unsafe_allow_html=True)
        with rt2:
            st.markdown(f"""
            <div class="kpi-card orange">
              <div class="kpi-label">停電軒数（管内合計）</div>
              <div class="kpi-value">{total_count:,}</div>
              <div class="kpi-sub">軒</div>
            </div>""", unsafe_allow_html=True)
        with rt3:
            if max_row is not None and int(max_row["affected_customers"]) > 0:
                mx_name = max_row["prefecture"]
                mx_val  = int(max_row["affected_customers"])
                val_str = f"{mx_val:,} 軒"
            else:
                mx_name, val_str = "停電なし", "全県停電なし"
            st.markdown(f"""
            <div class="kpi-card blue">
              <div class="kpi-label">最多停電県</div>
              <div class="kpi-value" style="font-size:1.3rem;">{mx_name}</div>
              <div class="kpi-sub">{val_str}</div>
            </div>""", unsafe_allow_html=True)

        st.markdown("")

        # 棒グラフ
        st.markdown('<div class="section-title">都道府県別 現在の停電軒数</div>',
                    unsafe_allow_html=True)
        rows_bar = []
        for p in pref_order:
            r = comp_rt[comp_rt["prefecture"] == p]
            v = int(r["affected_customers"].iloc[0]) if not r.empty and \
                r.iloc[0]["data_status"] == "取得済み" else 0
            s = r.iloc[0]["data_status"] if not r.empty else "取得不可"
            rows_bar.append({"都道府県": p, "停電軒数": v,
                             "状態": "停電中" if v > 0 else ("取得不可" if s == "取得不可" else "停電なし")})
        df_bar = pd.DataFrame(rows_bar)
        color_map = {"停電中": "#ef4444", "停電なし": "#4ade80", "取得不可": "#cbd5e1"}
        fig_bar = px.bar(df_bar, x="都道府県", y="停電軒数", color="状態",
                         color_discrete_map=color_map, text="停電軒数",
                         category_orders={"都道府県": pref_order})
        fig_bar.update_traces(texttemplate="%{text:,}", textposition="outside")
        fig_bar.update_layout(
            height=320, margin=dict(t=20, b=20),
            plot_bgcolor="white", paper_bgcolor="white",
            xaxis=dict(showgrid=False), yaxis=dict(gridcolor="#f3f4f6", tickformat=","),
            showlegend=True, legend=dict(orientation="h", y=1.08),
        )
        st.plotly_chart(fig_bar, use_container_width=True)

        # 都道府県カード
        st.markdown('<div class="section-title">都道府県別 詳細</div>', unsafe_allow_html=True)
        _pref_cards(pref_order, pref_colors, comp_rt, key_prefix)

    # ── サブタブ B: 履歴分析 ─────────────────────────────────
    with sub_hist:
        comp_hist = df_hist[df_hist["company"] == company_name].copy()

        if comp_hist.empty:
            st.warning(f"{company_name} の履歴データが取得できませんでした。")
            st.markdown(
                f'<div style="font-size:0.8rem; color:#6b7280;">'
                f'データ取得元: <a href="{hist_url}" target="_blank" style="color:#3b82f6;">'
                f'{company_name} 停電履歴ページ</a></div>',
                unsafe_allow_html=True,
            )
            return

        # weather_flag 補完
        if "weather_flag" not in comp_hist.columns:
            comp_hist["weather_flag"] = "不明"

        # フィルター
        prefs_avail = [p for p in pref_order if p in comp_hist["prefecture"].unique()]
        prefs_sel   = ["全県"] + prefs_avail
        sel_pref = st.radio("都道府県（ワンクリック）", prefs_sel, horizontal=True,
                            key=f"{key_prefix}_pref")
        fh1, fh2 = st.columns(2)
        with fh1:
            cats = ["全カテゴリー"] + sorted(comp_hist["cause_category"].unique().tolist())
            sel_cat = st.selectbox("起因カテゴリー", cats, key=f"{key_prefix}_cat")
        with fh2:
            weather_opts = ["全て（絞り込まない）"] + list(WEATHER_FLAG_CONFIG.keys())
            sel_weather  = st.selectbox("起因フラグ フィルター", weather_opts,
                                        key=f"{key_prefix}_weather")

        dfc = comp_hist.copy()
        if sel_pref  != "全県":            dfc = dfc[dfc["prefecture"]     == sel_pref]
        if sel_cat   != "全カテゴリー":    dfc = dfc[dfc["cause_category"] == sel_cat]
        if sel_weather != "全て（絞り込まない）":
            dfc = dfc[dfc["weather_flag"].str.contains(sel_weather, regex=False, na=False)]

        # 起因フラグ サマリーバー
        wf_counts, total_cnt = _weather_summary_bar(dfc)

        # KPI
        dfc = dfc.copy()
        dfc["_primary_flag"] = dfc["weather_flag"].str.split("|").str[0]

        hk1, hk2, hk3, hk4 = st.columns(4)
        with hk1: st.metric("停電件数",    f"{dfc['incidents'].sum():,} 件")
        with hk2: st.metric("停電軒数合計", f"{dfc['affected_customers'].sum():,} 軒")
        with hk3: st.metric("停電時間合計", f"{dfc['total_outage_hours'].sum():,.1f} h")
        with hk4:
            nat_rows = dfc["weather_flag"].apply(
                lambda x: any(f in str(x).split("|") for f in ["天候", "樹木・倒木"])
            ).sum()
            nat_pct = nat_rows / total_cnt * 100 if total_cnt > 0 else 0
            st.metric("自然起因の割合", f"{nat_pct:.0f} %",
                      help="天候 または 樹木・倒木 フラグを含む件数の割合")

        st.markdown("")

        # グラフ行1: 日別トレンド + 起因フラグドーナツ
        g1, g2 = st.columns([3, 2])
        with g1:
            st.markdown('<div class="section-title">日別 停電件数推移（起因フラグ別）</div>',
                        unsafe_allow_html=True)
            wcolor = {k: v["badge_bg"] for k, v in WEATHER_FLAG_CONFIG.items()}
            daily_w = (
                dfc.groupby(["date_label", "_primary_flag"])["incidents"]
                .sum().reset_index()
                .rename(columns={"incidents": "件数", "_primary_flag": "起因フラグ"})
                .sort_values("date_label")
            )
            fig_daily = px.bar(daily_w, x="date_label", y="件数", color="起因フラグ",
                               color_discrete_map=wcolor, barmode="stack",
                               labels={"date_label": "発生日", "件数": "停電件数"})
            fig_daily.update_layout(
                height=310, hovermode="x unified",
                legend=dict(orientation="h", y=1.1, title=""),
                margin=dict(t=10, b=10), plot_bgcolor="white", paper_bgcolor="white",
            )
            fig_daily.update_xaxes(showgrid=False, tickangle=-45)
            fig_daily.update_yaxes(gridcolor="#f3f4f6")
            st.plotly_chart(fig_daily, use_container_width=True)

        with g2:
            st.markdown('<div class="section-title">起因フラグ 内訳</div>',
                        unsafe_allow_html=True)
            wf_df = pd.DataFrame(
                [{"区分": k, "件数": v} for k, v in wf_counts.items()
                 if k in WEATHER_FLAG_CONFIG]
            )
            if not wf_df.empty:
                fig_wpie = go.Figure(go.Pie(
                    labels=wf_df["区分"], values=wf_df["件数"],
                    marker_colors=[WEATHER_FLAG_CONFIG.get(f, {}).get("badge_bg", "#9ca3af")
                                   for f in wf_df["区分"]],
                    hole=0.5, textinfo="percent+label",
                    hovertemplate="<b>%{label}</b><br>%{value}件 (%{percent})<extra></extra>",
                ))
                fig_wpie.update_layout(height=310, margin=dict(t=10, b=10), showlegend=False)
                st.plotly_chart(fig_wpie, use_container_width=True)

        # グラフ行2: 都道府県別 + 起因原文 Top10
        g3, g4 = st.columns(2)
        with g3:
            st.markdown('<div class="section-title">都道府県別 停電件数・軒数</div>',
                        unsafe_allow_html=True)
            pref_agg = (
                dfc.groupby("prefecture")
                .agg(件数=("incidents", "sum"), 軒数=("affected_customers", "sum"))
                .reindex(pref_order).fillna(0).reset_index()
            )
            fig_pref_d = make_subplots(specs=[[{"secondary_y": True}]])
            fig_pref_d.add_trace(
                go.Bar(x=pref_agg["prefecture"], y=pref_agg["軒数"],
                       name="停電軒数",
                       marker_color=[pref_colors.get(p, "#9ca3af")
                                     for p in pref_agg["prefecture"]],
                       opacity=0.8),
                secondary_y=False,
            )
            fig_pref_d.add_trace(
                go.Scatter(x=pref_agg["prefecture"], y=pref_agg["件数"],
                           name="停電件数", mode="markers+lines",
                           marker=dict(size=8, color="#7c3aed"),
                           line=dict(color="#7c3aed", width=2)),
                secondary_y=True,
            )
            fig_pref_d.update_layout(
                height=310, hovermode="x unified",
                legend=dict(orientation="h", y=1.1),
                margin=dict(t=10, b=10), plot_bgcolor="white", paper_bgcolor="white",
            )
            fig_pref_d.update_xaxes(showgrid=False)
            fig_pref_d.update_yaxes(title_text="停電軒数", secondary_y=False,
                                    gridcolor="#f3f4f6", tickformat=",")
            fig_pref_d.update_yaxes(title_text="停電件数", secondary_y=True, showgrid=False)
            st.plotly_chart(fig_pref_d, use_container_width=True)

        with g4:
            st.markdown('<div class="section-title">起因（原文）別 件数 Top10</div>',
                        unsafe_allow_html=True)
            raw_agg = (
                dfc.groupby(["raw_reason", "_primary_flag"])["incidents"]
                .sum().reset_index()
                .rename(columns={"incidents": "件数"})
                .sort_values("件数", ascending=True).tail(10)
            )
            fig_raw = go.Figure(go.Bar(
                x=raw_agg["件数"], y=raw_agg["raw_reason"],
                orientation="h",
                marker_color=[WEATHER_FLAG_CONFIG.get(f, {}).get("badge_bg", "#9ca3af")
                              for f in raw_agg["_primary_flag"]],
                text=raw_agg["件数"], textposition="outside",
            ))
            fig_raw.update_layout(
                height=310, margin=dict(t=10, b=10, r=40),
                plot_bgcolor="white", paper_bgcolor="white",
                xaxis=dict(gridcolor="#f3f4f6"), yaxis=dict(showgrid=False),
            )
            st.plotly_chart(fig_raw, use_container_width=True)

        # 都道府県 × 日付 ヒートマップ
        if len(pref_order) > 1:
            st.markdown('<div class="section-title">都道府県 × 発生日 停電件数ヒートマップ</div>',
                        unsafe_allow_html=True)
            heat = (
                dfc.groupby(["prefecture", "date_label"])["incidents"]
                .sum().reset_index()
                .pivot(index="prefecture", columns="date_label", values="incidents").fillna(0)
                .reindex([p for p in pref_order if p in dfc["prefecture"].unique()])
            )
            if not heat.empty:
                fig_ht = go.Figure(go.Heatmap(
                    z=heat.values, x=heat.columns.tolist(), y=heat.index.tolist(),
                    colorscale="Blues",
                    hovertemplate="<b>%{y}</b>  %{x}<br>停電件数: %{z:.0f}件<extra></extra>",
                    colorbar=dict(title="停電件数"),
                ))
                fig_ht.update_layout(
                    height=280, margin=dict(t=10, b=10),
                    xaxis=dict(showgrid=False, tickangle=-45),
                    yaxis=dict(showgrid=False),
                )
                st.plotly_chart(fig_ht, use_container_width=True)

        # 詳細テーブル
        st.markdown(
            '<div class="section-title">停電記録一覧 ※列ヘッダーをクリックでソート</div>',
            unsafe_allow_html=True,
        )
        disp_cols = {
            "date_label":         "発生日",
            "prefecture":         "都道府県",
            "raw_reason":         "起因（原文）",
            "weather_flag":       "起因フラグ",
            "affected_customers": "停電軒数",
            "total_outage_hours": "停電時間(h)",
            "cause_category":     "カテゴリー",
        }
        disp = (
            dfc[[c for c in disp_cols if c in dfc.columns]]
            .rename(columns={c: v for c, v in disp_cols.items() if c in dfc.columns})
            .sort_values(["発生日", "都道府県"], ascending=[False, True])
            .reset_index(drop=True)
        )
        _TH = "background:#dbeafe; color:#1e3a8a; font-size:0.75rem; font-weight:700; padding:8px 10px; white-space:nowrap;"
        _TD_g = "border-bottom:1px solid #f0f4ff; padding:8px 10px; font-size:0.8rem; vertical-align:top;"
        head_html = "".join(f'<th style="{_TH}">{c}</th>' for c in disp.columns)
        rows_html  = ""
        for ri, row in disp.iterrows():
            bg = "#fafcff" if ri % 2 == 0 else "white"
            cells = ""
            for col, val in row.items():
                cells += f'<td style="{_TD_g}">{_html.escape(str(val))}</td>'
            rows_html += f'<tr style="background:{bg};">{cells}</tr>'
        st.markdown(
            '<div style="overflow:auto; max-height:460px; border:1px solid #dbeafe;'
            ' border-radius:10px; background:white;">'
            '<table style="width:100%; border-collapse:collapse;">'
            f"<thead><tr>{head_html}</tr></thead>"
            f"<tbody>{rows_html}</tbody>"
            "</table></div>",
            unsafe_allow_html=True,
        )

        st.markdown(
            f'<div style="font-size:0.75rem; color:#9ca3af; margin-top:8px;">'
            f'データ取得元: <a href="{hist_url}" target="_blank" style="color:#3b82f6;">'
            f'{company_name} 停電履歴ページ</a>'
            f'&ensp;|&ensp;対象期間: {n_hist_days}</div>',
            unsafe_allow_html=True,
        )


# ─── タブ ─────────────────────────────────────────────────────
tab_rt, tab_cause, tab_companies, tab_hist = st.tabs([
    "🔴 リアルタイム停電情報",
    "🔍 事故起因 集計（実データ）",
    "🏢 各社 詳細",
    "📊 過去1年の停電実績（参考）",
])


# ═══════════════════════════════════════════════════
# タブ1: リアルタイム
# ═══════════════════════════════════════════════════
with tab_rt:
    with st.spinner("各電力ネットワーク会社から情報を取得中..."):
        df_rt = load_realtime_data()

    active  = df_rt[df_rt["affected_customers"] > 0]
    ok_cnt  = (df_rt["data_status"] == "取得済み").sum()
    ng_cnt  = (df_rt["data_status"] == "取得不可").sum()

    # ── データカバレッジ表示 ─────────────────────────
    st.markdown(coverage_html(df_rt), unsafe_allow_html=True)

    # ── KPI ─────────────────────────────────────────
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.markdown(f"""
        <div class="kpi-card red">
          <div class="kpi-label">停電中都道府県数</div>
          <div class="kpi-value">{len(active)}</div>
          <div class="kpi-sub">（確認済み {ok_cnt} 都道府県中）</div>
        </div>""", unsafe_allow_html=True)
    with k2:
        total = active["affected_customers"].sum()
        st.markdown(f"""
        <div class="kpi-card orange">
          <div class="kpi-label">停電軒数（合計）</div>
          <div class="kpi-value">{total:,}</div>
          <div class="kpi-sub">軒（取得済み地域のみ）</div>
        </div>""", unsafe_allow_html=True)
    with k3:
        st.markdown(f"""
        <div class="kpi-card green">
          <div class="kpi-label">停電なし確認済み</div>
          <div class="kpi-value">{ok_cnt - len(active)}</div>
          <div class="kpi-sub">都道府県</div>
        </div>""", unsafe_allow_html=True)
    with k4:
        st.markdown(f"""
        <div class="kpi-card gray">
          <div class="kpi-label">データ未取得</div>
          <div class="kpi-value">{ng_cnt}</div>
          <div class="kpi-sub">都道府県（各社HP参照）</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("")

    # ── 凡例バー ───────────────────────────────────
    st.markdown("""
    <div class="legend-bar">
      <b>停電軒数（確認済みエリア）</b>
      <div class="legend-item"><div class="dot" style="background:#4ade80"></div>停電なし</div>
      <div class="legend-item"><div class="dot" style="background:#fbbf24"></div>〜1,000軒</div>
      <div class="legend-item"><div class="dot" style="background:#f97316"></div>1,001〜10,000軒</div>
      <div class="legend-item"><div class="dot" style="background:#dc2626"></div>10,001軒以上</div>
      <div class="legend-item"><div class="dot" style="background:#cbd5e1"></div>データ未取得</div>
    </div>
    """, unsafe_allow_html=True)

    # ── マップ + 都道府県リスト ─────────────────────
    col_map, col_list = st.columns([3, 2])
    with col_map:
        geojson = load_japan_geojson()
        if geojson:
            st.plotly_chart(build_choropleth(df_rt, geojson), use_container_width=True)
        else:
            st.info("地図データ取得中…バブルマップで表示します。")
            st.plotly_chart(build_bubble_map(df_rt), use_container_width=True)

    with col_list:
        confirmed_active = len(active)
        st.markdown(
            f'<div class="section-title">停電状況リスト'
            f'<span style="font-size:0.8rem; font-weight:400; color:#6b7280; margin-left:8px;">'
            f'停電中: {confirmed_active}都道府県</span></div>',
            unsafe_allow_html=True,
        )
        st.markdown(pref_list_html(df_rt), unsafe_allow_html=True)

    # ── データ取得元リンク ───────────────────────────
    st.markdown('<div class="section-title">データ取得元（各電力ネットワーク会社公式サイト）</div>',
                unsafe_allow_html=True)
    sources = [
        ("北海道電力ネットワーク", "https://teiden-info.hepco.co.jp/",                  "北海道",                                         True),
        ("東北電力ネットワーク",   "https://nw.tohoku-epco.co.jp/teideninfo/",          "青森/岩手/宮城/秋田/山形/福島/新潟",             True),
        ("北陸電力送配電",         "https://www.rikuden.co.jp/nw/teiden/otj010.html",   "富山/石川/福井",                                 True),
        ("中部電力パワーグリッド", "https://teiden.powergrid.chuden.co.jp/p/index.html", "愛知/長野（+三重/岐阜/静岡は他社分と重複）",    True),
        ("東京電力パワーグリッド", "https://teideninfo.tepco.co.jp/",                    "茨城/栃木/群馬/埼玉/千葉/東京/神奈川/山梨/静岡", True),
        ("関西電力送配電",         "https://www.kansai-td.co.jp/teiden-info/index.php",  "滋賀/京都/大阪/兵庫/奈良/和歌山/福井/岐阜/三重", True),
        ("四国電力送配電",         "https://www.yonden.co.jp/nw/teiden-info/index.html", "香川/愛媛/徳島/高知",                            True),
        ("中国電力ネットワーク",   "https://www.teideninfo.energia.co.jp/",              "鳥取/島根/岡山/広島/山口",                       True),
        ("九州電力送配電",         "https://www.kyuden.co.jp/td_teiden/kyushu.html",     "福岡/佐賀/長崎/熊本/大分/宮崎/鹿児島",          True),
        ("沖縄電力",               "https://www.okidenmail.jp/bosai/info/index.html",    "沖縄",                                           True),
    ]
    cols = st.columns(2)
    for i, (name, url, prefs, available) in enumerate(sources):
        badge = "✅ 取得済み" if available else "⬜ 直接確認"
        color = "#dcfce7" if available else "#f1f5f9"
        fc    = "#166534" if available else "#64748b"
        with cols[i % 2]:
            st.markdown(
                f'<div style="background:{color}; border-radius:8px; padding:10px 14px; margin-bottom:8px;">'
                f'<span style="font-size:0.72rem; font-weight:700; color:{fc}">{badge}</span> '
                f'<a href="{url}" target="_blank" style="font-weight:600; font-size:0.88rem; color:#1e293b; text-decoration:none;">{name}</a>'
                f'<div style="font-size:0.7rem; color:#6b7280; margin-top:3px;">{prefs}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )


# ═══════════════════════════════════════════════════
# タブ2: 事故起因 集計（実データ）
# ═══════════════════════════════════════════════════
with tab_cause:
    with st.spinner("停電履歴・起因データを取得中..."):
        df_hist_c = load_history_data()

    # ── データカバレッジ表示 ─────────────────────────────────────
    if df_hist_c.empty:
        st.warning("履歴データの取得に失敗しました。各社公式サイトをご参照ください。")
    else:
        covered = sorted(df_hist_c["company"].unique().tolist())
        covered_tags = "".join(
            f'<span class="coverage-tag tag-ok">✓ {c}</span>' for c in covered
        )
        ng_companies = [
            "中部電力パワーグリッド（起因なし）",
        ]
        ng_tags = "".join(
            f'<span class="coverage-tag tag-ng">✕ {c}</span>' for c in ng_companies
        )
        st.markdown(
            f'<div class="coverage-bar">'
            f'<b style="font-size:0.78rem">実データカバレッジ: {len(covered)}社</b>'
            f'{covered_tags}{ng_tags}'
            f'</div>',
            unsafe_allow_html=True,
        )

        period_start = df_hist_c["month_label"].min() if not df_hist_c.empty else "—"
        period_end   = df_hist_c["month_label"].max() if not df_hist_c.empty else "—"
        st.markdown(
            f"**対象期間:** {period_start} ～ {period_end}&ensp;|&ensp;"
            f"取得元: 北海道電力（過去7日）・東北電力（過去31日）・北陸電力（過去7日）・東京電力PG（過去60日）"
            f"・関西電力（過去7日）・四国電力（件数のみ）・中国電力（過去7日）・九州電力（過去7日）・沖縄電力（過去数日）",
            help="四国電力は起因情報なし（起因=「不明」として集計）。中部電力PGはリアルタイムのみ取得・履歴起因情報は非公開のため集計対象外。"
                 "北陸電力はリアルタイム件数の都道府県別集計は対応外（停電有無のみ）。",
        )

        # ── フィルター ───────────────────────────────────────────
        # 電力会社: ワンクリック横並びラジオ
        companies_c = ["全電力会社"] + sorted(df_hist_c["company"].unique().tolist())
        sel_company_c = st.radio(
            "電力会社（ワンクリックで絞り込み）",
            companies_c,
            horizontal=True,
            key="cause_company",
        )

        fc1, fc2 = st.columns(2)
        with fc1:
            regions_c = ["全地域"] + sorted(
                [r for r in df_hist_c["region"].unique().tolist() if r]
            )
            sel_region_c = st.selectbox("地域", regions_c, key="cause_region")
        with fc2:
            cats_c = ["全カテゴリー"] + sorted(df_hist_c["cause_category"].unique().tolist())
            sel_cat = st.selectbox("起因カテゴリー", cats_c, key="cause_cat")

        dfc = df_hist_c.copy()
        if sel_company_c != "全電力会社":   dfc = dfc[dfc["company"]       == sel_company_c]
        if sel_region_c  != "全地域":       dfc = dfc[dfc["region"]        == sel_region_c]
        if sel_cat       != "全カテゴリー": dfc = dfc[dfc["cause_category"] == sel_cat]

        # ── KPI ─────────────────────────────────────────────────
        hk1, hk2, hk3, hk4 = st.columns(4)
        with hk1: st.metric("停電件数（実績）",   f"{dfc['incidents'].sum():,} 件")
        with hk2: st.metric("停電軒数（実績）",   f"{dfc['affected_customers'].sum():,} 軒")
        with hk3: st.metric("停電時間（実績）",   f"{dfc['total_outage_hours'].sum():,.1f} 時間")
        with hk4:
            top_c = (
                dfc[dfc["cause"] != "不明"]
                .groupby("cause")["incidents"].sum().idxmax()
                if not dfc[dfc["cause"] != "不明"].empty else "—"
            )
            st.metric("最多起因（件数）", top_c)

        st.markdown("")

        # ── グラフ（2列）────────────────────────────────────────
        col_d1, col_d2 = st.columns(2)

        with col_d1:
            st.markdown('<div class="section-title">起因カテゴリー別 停電件数</div>',
                        unsafe_allow_html=True)
            cat_h = dfc.groupby("cause_category")["incidents"].sum().reset_index()
            cat_colors = [CATEGORY_COLOR.get(c, "#9ca3af") for c in cat_h["cause_category"]]
            fig_cat = go.Figure(go.Pie(
                labels=cat_h["cause_category"], values=cat_h["incidents"],
                marker_colors=cat_colors, hole=0.45, textinfo="percent+label",
            ))
            fig_cat.update_layout(height=340, margin=dict(t=10, b=10), showlegend=False)
            st.plotly_chart(fig_cat, use_container_width=True)

        with col_d2:
            st.markdown('<div class="section-title">事故起因別 停電件数（Top10）</div>',
                        unsafe_allow_html=True)
            cause_h = (
                dfc.groupby(["cause", "cause_category"])["incidents"]
                .sum().reset_index()
                .sort_values("incidents", ascending=True).tail(10)
            )
            bar_colors = [CATEGORY_COLOR.get(c, "#9ca3af") for c in cause_h["cause_category"]]
            fig_ch = go.Figure(go.Bar(
                x=cause_h["incidents"], y=cause_h["cause"],
                orientation="h", marker_color=bar_colors,
                text=cause_h["incidents"].apply(lambda x: f"{x:,}"),
                textposition="outside",
            ))
            fig_ch.update_layout(
                height=340, margin=dict(t=10, b=10, r=50),
                plot_bgcolor="white", paper_bgcolor="white",
                xaxis=dict(gridcolor="#f3f4f6", tickformat=","),
                yaxis=dict(showgrid=False),
            )
            st.plotly_chart(fig_ch, use_container_width=True)

        # ── 日別 × 起因カテゴリー推移 ───────────────────────────
        st.markdown('<div class="section-title">日別 × 事故起因カテゴリー 停電件数の推移</div>',
                    unsafe_allow_html=True)
        if not dfc.empty:
            # date_label（YYYY/MM/DD）が使えれば日単位、なければ month_label
            if "date_label" in dfc.columns and dfc["date_label"].ne("").any():
                group_col  = "date_label"
                x_label    = "発生日"
            else:
                group_col  = "month_label"
                x_label    = "月"
            cm = dfc.groupby([group_col, "cause_category"])["incidents"].sum().reset_index()
            fig_stack = px.bar(
                cm, x=group_col, y="incidents", color="cause_category",
                color_discrete_map=CATEGORY_COLOR, barmode="stack",
                labels={group_col: x_label, "incidents": "停電件数",
                        "cause_category": "起因カテゴリー"},
            )
            fig_stack.update_layout(
                height=360, margin=dict(t=20, b=20),
                plot_bgcolor="white", paper_bgcolor="white",
                legend=dict(orientation="h", y=1.08), hovermode="x unified",
            )
            fig_stack.update_xaxes(showgrid=False, tickangle=-45)
            fig_stack.update_yaxes(gridcolor="#f3f4f6")
            st.plotly_chart(fig_stack, use_container_width=True)

        # ── 事故起因 × 地域 ヒートマップ ────────────────────────
        st.markdown('<div class="section-title">事故起因 × 地域 停電件数ヒートマップ</div>',
                    unsafe_allow_html=True)
        pivot = (
            dfc[dfc["region"] != ""]
            .groupby(["cause", "region"])["incidents"]
            .sum().reset_index()
            .pivot(index="cause", columns="region", values="incidents").fillna(0)
        )
        if not pivot.empty:
            fig_hm = go.Figure(go.Heatmap(
                z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(),
                colorscale="YlOrRd",
                hovertemplate="<b>%{y}</b> × %{x}<br>停電件数: %{z:,}件<extra></extra>",
                colorbar=dict(title="停電件数"),
            ))
            fig_hm.update_layout(
                height=500, margin=dict(t=10, b=10),
                xaxis=dict(showgrid=False),
                yaxis=dict(showgrid=False, autorange="reversed"),
            )
            st.plotly_chart(fig_hm, use_container_width=True)

        # ── 詳細テーブル：起因別サマリー ─────────────────────────
        st.markdown('<div class="section-title">事故起因別 実績サマリー</div>',
                    unsafe_allow_html=True)
        tbl = (
            dfc.groupby(["cause_category", "cause"])
            .agg(
                停電件数=("incidents", "sum"),
                停電軒数=("affected_customers", "sum"),
                停電時間=("total_outage_hours", "sum"),
                対象都道府県数=("prefecture", "nunique"),
            )
            .reset_index()
            .sort_values(["cause_category", "停電件数"], ascending=[True, False])
            .rename(columns={"cause_category": "カテゴリー", "cause": "事故起因"})
        )
        tbl["停電時間"] = tbl["停電時間"].round(1)

        def style_cat(row):
            bg = {
                "自然災害":     "#fef2f2",
                "設備・機器":   "#fff7ed",
                "外的要因":     "#faf5ff",
                "計画停電":     "#eff6ff",
                "不明・その他": "#f9fafb",
            }.get(row["カテゴリー"], "white")
            return [f"background-color:{bg}"] * len(row)

        st.dataframe(
            tbl.style.apply(style_cat, axis=1)
               .format({"停電件数": "{:,}", "停電軒数": "{:,}", "停電時間": "{:,.1f}"}),
            use_container_width=True, height=400,
        )

        # ── 生データテーブル ─────────────────────────────────────
        with st.expander("📋 生データ（停電記録一覧）を表示 ※列ヘッダーをクリックでソート",
                         expanded=False):
            # date_label がない（旧キャッシュ）場合は month_label で代替
            if "date_label" in dfc.columns:
                show_date = dfc["date_label"].where(dfc["date_label"] != "", dfc["month_label"])
            else:
                show_date = dfc["month_label"]

            disp = dfc.assign(発生日=show_date)[[
                "発生日", "company", "prefecture", "region",
                "cause_category", "cause", "raw_reason",
                "affected_customers", "total_outage_hours",
            ]].rename(columns={
                "company":            "電力会社",
                "prefecture":         "都道府県",
                "region":             "地域",
                "cause_category":     "カテゴリー",
                "cause":              "起因（標準）",
                "raw_reason":         "起因（原文）",
                "affected_customers": "停電軒数",
                "total_outage_hours": "停電時間(h)",
            }).sort_values(["電力会社", "発生日"], ascending=[True, False])

            st.dataframe(
                disp.reset_index(drop=True),
                use_container_width=True,
                height=400,
                column_config={
                    "発生日":       st.column_config.TextColumn("発生日",     width="small"),
                    "電力会社":     st.column_config.TextColumn("電力会社",   width="medium"),
                    "都道府県":     st.column_config.TextColumn("都道府県",   width="small"),
                    "地域":         st.column_config.TextColumn("地域",       width="small"),
                    "カテゴリー":   st.column_config.TextColumn("カテゴリー", width="small"),
                    "起因（標準）": st.column_config.TextColumn("起因（標準）", width="medium"),
                    "起因（原文）": st.column_config.TextColumn("起因（原文）", width="large"),
                    "停電軒数":     st.column_config.NumberColumn("停電軒数",  format="%d 軒",  width="small"),
                    "停電時間(h)":  st.column_config.NumberColumn("停電時間(h)", format="%.2f h", width="small"),
                },
            )


# ═══════════════════════════════════════════════════
# タブ3: 各社 詳細
# ═══════════════════════════════════════════════════
with tab_companies:
    (ct_tohoku, ct_hokkaido, ct_rikuden, ct_chubu,
     ct_tepco, ct_kansai, ct_shikoku, ct_chugoku,
     ct_kyushu, ct_okinawa) = st.tabs([
        "🏔️ 東北電力NW",
        "🌨️ 北海道電力NW",
        "⛰️ 北陸電力",
        "🏭 中部電力PG",
        "🗼 東京電力PG",
        "⛩️ 関西電力",
        "🌊 四国電力",
        "🏯 中国電力NW",
        "🌸 九州電力",
        "🌺 沖縄電力",
    ])

    _df_rt_comp   = load_realtime_data()
    _df_hist_comp = load_history_data()

    # ── 東北電力NW (既存の詳細実装) ──────────────────────────
    with ct_tohoku:
        sub_rt, sub_hist = st.tabs(["📡 リアルタイム状況", "📅 過去31日の履歴分析"])

        # ── サブタブ A: リアルタイム ────────────────────────────────
        with sub_rt:
            with st.spinner("東北電力ネットワークから情報を取得中..."):
                t_counts, t_ts = load_tohoku_realtime()

            st.markdown(
                f'<div style="font-size:0.8rem; color:#6b7280; margin-bottom:12px;">'
                f'情報更新: <b>{t_ts or "—"}</b>&ensp;|&ensp;'
                f'<a href="https://nw.tohoku-epco.co.jp/teideninfo/" target="_blank" style="color:#3b82f6">'
                f'東北電力ネットワーク 停電情報ページ</a></div>',
                unsafe_allow_html=True,
            )

            # KPI
            active_prefs = {k: v for k, v in t_counts.items() if v and v > 0}
            total_t      = sum(v for v in t_counts.values() if v)
            rt1, rt2, rt3 = st.columns(3)
            with rt1:
                st.markdown(f"""
                <div class="kpi-card red">
                  <div class="kpi-label">停電中県数</div>
                  <div class="kpi-value">{len(active_prefs)}</div>
                  <div class="kpi-sub">/ 7 県（管内）</div>
                </div>""", unsafe_allow_html=True)
            with rt2:
                st.markdown(f"""
                <div class="kpi-card orange">
                  <div class="kpi-label">停電軒数（管内合計）</div>
                  <div class="kpi-value">{total_t:,}</div>
                  <div class="kpi-sub">軒</div>
                </div>""", unsafe_allow_html=True)
            with rt3:
                max_pref = max(t_counts, key=lambda k: t_counts.get(k) or 0) if t_counts else "—"
                max_val  = t_counts.get(max_pref, 0) or 0
                st.markdown(f"""
                <div class="kpi-card blue">
                  <div class="kpi-label">最多停電県</div>
                  <div class="kpi-value" style="font-size:1.3rem">{max_pref if max_val > 0 else "停電なし"}</div>
                  <div class="kpi-sub">{f"{max_val:,} 軒" if max_val > 0 else "全県停電なし"}</div>
                </div>""", unsafe_allow_html=True)

            st.markdown("")

            # 都道府県別棒グラフ
            st.markdown('<div class="section-title">都道府県別 現在の停電軒数</div>',
                        unsafe_allow_html=True)
            rows_t = []
            for p in _TOHOKU_PREF_ORDER:
                v = t_counts.get(p)
                rows_t.append({
                    "都道府県": p,
                    "停電軒数": v if v is not None else 0,
                    "状態":     "停電中" if (v and v > 0) else ("取得不可" if v is None else "停電なし"),
                })
            df_rt_t = pd.DataFrame(rows_t)
            color_map = {"停電中": "#ef4444", "停電なし": "#4ade80", "取得不可": "#cbd5e1"}
            fig_rt_bar = px.bar(
                df_rt_t, x="都道府県", y="停電軒数", color="状態",
                color_discrete_map=color_map,
                text="停電軒数",
                category_orders={"都道府県": _TOHOKU_PREF_ORDER},
            )
            fig_rt_bar.update_traces(texttemplate="%{text:,}", textposition="outside")
            fig_rt_bar.update_layout(
                height=340, margin=dict(t=20, b=20),
                plot_bgcolor="white", paper_bgcolor="white",
                xaxis=dict(showgrid=False), yaxis=dict(gridcolor="#f3f4f6", tickformat=","),
                showlegend=True, legend=dict(orientation="h", y=1.08),
            )
            st.plotly_chart(fig_rt_bar, use_container_width=True)

            # 都道府県カード
            st.markdown('<div class="section-title">都道府県別 詳細</div>', unsafe_allow_html=True)
            cols_p = st.columns(7)
            for idx, pref in enumerate(_TOHOKU_PREF_ORDER):
                v = t_counts.get(pref)
                if v is None:
                    bg, txt, val_str = "#f8fafc", "#64748b", "取得不可"
                elif v == 0:
                    bg, txt, val_str = "#f0fdf4", "#16a34a", "0 軒"
                elif v <= 1000:
                    bg, txt, val_str = "#fefce8", "#ca8a04", f"{v:,} 軒"
                elif v <= 10000:
                    bg, txt, val_str = "#fff7ed", "#c2410c", f"{v:,} 軒"
                else:
                    bg, txt, val_str = "#fef2f2", "#b91c1c", f"{v:,} 軒"
                dot_c = _TOHOKU_PREF_COLOR.get(pref, "#6b7280")
                with cols_p[idx]:
                    st.markdown(
                        f'<div style="background:{bg}; border-radius:8px; padding:10px 8px;'
                        f' text-align:center; border:1px solid #e5e7eb;">'
                        f'<div style="width:10px;height:10px;border-radius:50%;background:{dot_c};'
                        f' margin:0 auto 4px;"></div>'
                        f'<div style="font-size:0.78rem; font-weight:700;">{pref}</div>'
                        f'<div style="font-size:1rem; font-weight:700; color:{txt};'
                        f' margin-top:4px;">{val_str}</div>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )

        # ── サブタブ B: 履歴分析 ────────────────────────────────────
        with sub_hist:
            with st.spinner("東北電力ネットワーク 過去31日データを取得中..."):
                df_th = load_tohoku_detail()

            if df_th.empty:
                st.warning("履歴データの取得に失敗しました。")
            else:
                # ── フィルター ──────────────────────────────────────
                prefs_sel = ["全県"] + _TOHOKU_PREF_ORDER
                sel_pref_t = st.radio("都道府県（ワンクリック）", prefs_sel,
                                      horizontal=True, key="tohoku_pref")

                fh2, fh3 = st.columns(2)
                with fh2:
                    cats_t = ["全カテゴリー"] + sorted(df_th["cause_category"].unique().tolist())
                    sel_cat_t = st.selectbox("起因カテゴリー", cats_t, key="tohoku_cat")
                with fh3:
                    weather_opts = ["全て（絞り込まない）"] + list(WEATHER_FLAG_CONFIG.keys())
                    sel_weather = st.selectbox("起因フラグ フィルター", weather_opts, key="tohoku_weather")

                dft = df_th.copy()
                if sel_pref_t != "全県":
                    dft = dft[dft["pref_name"] == sel_pref_t]
                if sel_cat_t != "全カテゴリー":
                    dft = dft[dft["cause_category"] == sel_cat_t]
                if sel_weather != "全て（絞り込まない）":
                    dft = dft[dft["weather_flag"].str.contains(sel_weather, regex=False, na=False)]

                # ── 起因フラグ サマリーバー ────────────────────────
                # weather_flag は "|" 区切り複数フラグ → 個別フラグごとに集計
                from collections import Counter as _Counter
                _all_flags: list[str] = []
                for _fs in dft["weather_flag"].fillna("不明"):
                    _all_flags.extend(_fs.split("|"))
                wf_counts = _Counter(_all_flags)   # {フラグ名: 件数}
                total_cnt = len(dft)
                w_tags = ""
                for flag, cfg in WEATHER_FLAG_CONFIG.items():
                    n   = wf_counts.get(flag, 0)
                    pct = f"{n/total_cnt*100:.0f}%" if total_cnt > 0 else "—"
                    w_tags += (
                        f'<span style="display:inline-flex; align-items:center; gap:6px;'
                        f' background:{cfg["bg"]}; color:{cfg["color"]}; border-radius:20px;'
                        f' padding:4px 14px; font-size:0.78rem; font-weight:700;'
                        f' margin-right:8px;">'
                        f'{cfg["label"]} <span style="font-size:1rem;">{n}件</span>'
                        f' <span style="opacity:.7;">({pct})</span></span>'
                    )
                st.markdown(
                    f'<div style="background:#f8fafc; border:1px solid #e5e7eb;'
                    f' border-radius:8px; padding:10px 14px; margin:10px 0 4px;">'
                    f'<span style="font-size:0.75rem; font-weight:700; color:#374151;'
                    f' margin-right:12px;">起因フラグ判定</span>{w_tags}</div>',
                    unsafe_allow_html=True,
                )

                # ── KPI ─────────────────────────────────────────
                # グラフ着色用にプライマリフラグ列を追加（"|"区切りの先頭フラグ）
                dft = dft.copy()
                dft["_primary_flag"] = dft["weather_flag"].str.split("|").str[0]

                valid_dur = dft["duration_h"].dropna()
                tk1, tk2, tk3, tk4 = st.columns(4)
                with tk1: st.metric("停電件数",    f"{len(dft):,} 件")
                with tk2: st.metric("停電軒数合計", f"{dft['count'].sum():,} 軒")
                with tk3: st.metric("停電時間合計", f"{valid_dur.sum():,.1f} h")
                with tk4:
                    nature_rows = dft["weather_flag"].apply(
                        lambda x: any(f in str(x).split("|") for f in ["天候", "樹木・倒木"])
                    ).sum()
                    nature_pct = nature_rows / total_cnt * 100 if total_cnt > 0 else 0
                    st.metric("自然起因の割合", f"{nature_pct:.0f} %",
                              help="天候 または 樹木・倒木 フラグを含む件数の割合")

                st.markdown("")

                # ── グラフ行1: 日別トレンド（起因プライマリフラグ別スタック）+ ドーナツ ──
                g1, g2 = st.columns([3, 2])

                with g1:
                    st.markdown('<div class="section-title">日別 停電件数推移（起因フラグ別）</div>',
                                unsafe_allow_html=True)
                    wcolor = {k: v["badge_bg"] for k, v in WEATHER_FLAG_CONFIG.items()}
                    daily_w = (
                        dft.groupby(["date_label", "_primary_flag"])["count"]
                        .count().reset_index()
                        .rename(columns={"count": "件数", "_primary_flag": "起因フラグ"})
                        .sort_values("date_label")
                    )
                    fig_daily = px.bar(
                        daily_w, x="date_label", y="件数", color="起因フラグ",
                        color_discrete_map=wcolor, barmode="stack",
                        labels={"date_label": "発生日", "件数": "停電件数"},
                    )
                    fig_daily.update_layout(
                        height=320, hovermode="x unified",
                        legend=dict(orientation="h", y=1.1, title=""),
                        margin=dict(t=10, b=10), plot_bgcolor="white", paper_bgcolor="white",
                    )
                    fig_daily.update_xaxes(showgrid=False, tickangle=-45)
                    fig_daily.update_yaxes(gridcolor="#f3f4f6")
                    st.plotly_chart(fig_daily, use_container_width=True)

                with g2:
                    st.markdown('<div class="section-title">起因フラグ 内訳（個別カウント）</div>',
                                unsafe_allow_html=True)
                    # wf_counts は Counter（個別フラグごとの集計）
                    wf_df = pd.DataFrame(
                        [{"区分": k, "件数": v} for k, v in wf_counts.items()
                         if k in WEATHER_FLAG_CONFIG]
                    )
                    if wf_df.empty:
                        st.info("データなし")
                    else:
                        fig_wpie = go.Figure(go.Pie(
                            labels=wf_df["区分"], values=wf_df["件数"],
                            marker_colors=[WEATHER_FLAG_CONFIG.get(f, {}).get("badge_bg", "#9ca3af")
                                           for f in wf_df["区分"]],
                            hole=0.5, textinfo="percent+label",
                            hovertemplate="<b>%{label}</b><br>%{value}件 (%{percent})<extra></extra>",
                        ))
                        fig_wpie.update_layout(height=320, margin=dict(t=10, b=10), showlegend=False)
                        st.plotly_chart(fig_wpie, use_container_width=True)

                # ── グラフ行2: 都道府県別 + 起因原文別 ──────────────
                g3, g4 = st.columns(2)

                with g3:
                    st.markdown('<div class="section-title">都道府県別 停電件数・軒数</div>',
                                unsafe_allow_html=True)
                    pref_agg = (
                        dft.groupby("pref_name")
                        .agg(件数=("count", "count"), 軒数=("count", "sum"))
                        .reindex(_TOHOKU_PREF_ORDER).fillna(0).reset_index()
                    )
                    fig_pref = make_subplots(specs=[[{"secondary_y": True}]])
                    fig_pref.add_trace(
                        go.Bar(x=pref_agg["pref_name"], y=pref_agg["軒数"],
                               name="停電軒数",
                               marker_color=[_TOHOKU_PREF_COLOR.get(p, "#9ca3af")
                                             for p in pref_agg["pref_name"]],
                               opacity=0.8),
                        secondary_y=False,
                    )
                    fig_pref.add_trace(
                        go.Scatter(x=pref_agg["pref_name"], y=pref_agg["件数"],
                                   name="停電件数", mode="markers+lines",
                                   marker=dict(size=8, color="#7c3aed"),
                                   line=dict(color="#7c3aed", width=2)),
                        secondary_y=True,
                    )
                    fig_pref.update_layout(
                        height=320, hovermode="x unified",
                        legend=dict(orientation="h", y=1.1),
                        margin=dict(t=10, b=10), plot_bgcolor="white", paper_bgcolor="white",
                    )
                    fig_pref.update_xaxes(showgrid=False)
                    fig_pref.update_yaxes(title_text="停電軒数", secondary_y=False,
                                          gridcolor="#f3f4f6", tickformat=",")
                    fig_pref.update_yaxes(title_text="停電件数", secondary_y=True, showgrid=False)
                    st.plotly_chart(fig_pref, use_container_width=True)

                with g4:
                    st.markdown('<div class="section-title">起因（原文）別 停電件数 Top10</div>',
                                unsafe_allow_html=True)
                    raw_agg = (
                        dft.groupby(["raw_reason", "_primary_flag"])["count"]
                        .count().reset_index()
                        .rename(columns={"count": "件数"})
                        .sort_values("件数", ascending=True).tail(10)
                    )
                    fig_raw = go.Figure(go.Bar(
                        x=raw_agg["件数"], y=raw_agg["raw_reason"],
                        orientation="h",
                        marker_color=[WEATHER_FLAG_CONFIG.get(f, {}).get("badge_bg", "#9ca3af")
                                      for f in raw_agg["_primary_flag"]],
                        text=raw_agg["件数"], textposition="outside",
                    ))
                    fig_raw.update_layout(
                        height=320, margin=dict(t=10, b=10, r=40),
                        plot_bgcolor="white", paper_bgcolor="white",
                        xaxis=dict(gridcolor="#f3f4f6"), yaxis=dict(showgrid=False),
                    )
                    st.plotly_chart(fig_raw, use_container_width=True)

                # ── 各県別トレンド ───────────────────────────────────
                st.markdown('<div class="section-title">各県別 停電トレンド（過去31日）</div>',
                            unsafe_allow_html=True)

                _W_COLORS = {k: v["badge_bg"] for k, v in WEATHER_FLAG_CONFIG.items()}

                # ── 比較折れ線グラフ（全7県 1チャート）────────────
                tab_cnt, tab_vol = st.tabs(["📈 停電件数（折れ線）", "📊 停電軒数（棒グラフ）"])

                with tab_cnt:
                    pref_daily_cnt = (
                        dft.groupby(["date_label", "pref_name"])["count"]
                        .count().reset_index()
                        .rename(columns={"count": "件数"})
                        .sort_values("date_label")
                    )
                    fig_pline = px.line(
                        pref_daily_cnt, x="date_label", y="件数", color="pref_name",
                        color_discrete_map=_TOHOKU_PREF_COLOR, markers=True,
                        labels={"date_label": "発生日", "件数": "停電件数", "pref_name": "都道府県"},
                    )
                    fig_pline.update_layout(
                        height=340, hovermode="x unified",
                        legend=dict(orientation="h", y=1.08, title=""),
                        margin=dict(t=20, b=10), plot_bgcolor="white", paper_bgcolor="white",
                    )
                    fig_pline.update_xaxes(showgrid=False, tickangle=-45)
                    fig_pline.update_yaxes(gridcolor="#f3f4f6", tickformat=",d")
                    st.plotly_chart(fig_pline, use_container_width=True)

                with tab_vol:
                    pref_daily_vol = (
                        dft.groupby(["date_label", "pref_name"])["count"]
                        .sum().reset_index()
                        .rename(columns={"count": "軒数"})
                        .sort_values("date_label")
                    )
                    fig_pbar = px.bar(
                        pref_daily_vol, x="date_label", y="軒数", color="pref_name",
                        color_discrete_map=_TOHOKU_PREF_COLOR, barmode="stack",
                        labels={"date_label": "発生日", "軒数": "停電軒数", "pref_name": "都道府県"},
                    )
                    fig_pbar.update_layout(
                        height=340, hovermode="x unified",
                        legend=dict(orientation="h", y=1.08, title=""),
                        margin=dict(t=20, b=10), plot_bgcolor="white", paper_bgcolor="white",
                    )
                    fig_pbar.update_xaxes(showgrid=False, tickangle=-45)
                    fig_pbar.update_yaxes(gridcolor="#f3f4f6", tickformat=",")
                    st.plotly_chart(fig_pbar, use_container_width=True)

                # ── 県別スモールマルチプル ─────────────────────────
                st.markdown(
                    '<div style="font-size:0.82rem; font-weight:700; color:#374151;'
                    ' margin:14px 0 8px;">各県の日別件数（起因フラグ色分け）と集計サマリー</div>',
                    unsafe_allow_html=True,
                )

                # df_th全体（県フィルターなし）から各県データを作成
                dft_all = df_th.copy()
                # weather/categoryフィルターは維持
                if sel_cat_t != "全カテゴリー":
                    dft_all = dft_all[dft_all["cause_category"] == sel_cat_t]
                if sel_weather != "全て（絞り込まない）":
                    dft_all = dft_all[dft_all["weather_flag"].str.contains(
                        sel_weather, regex=False, na=False)]

                # スモールマルチプル用プライマリフラグ列を追加
                dft_all = dft_all.copy()
                dft_all["_primary_flag"] = dft_all["weather_flag"].str.split("|").str[0]

                sm_cols = st.columns(4)
                for p_idx, pref in enumerate(_TOHOKU_PREF_ORDER):
                    pf = dft_all[dft_all["pref_name"] == pref]
                    dot_color = _TOHOKU_PREF_COLOR.get(pref, "#6b7280")

                    # ミニバーチャートデータ（プライマリフラグで着色）
                    mini_data = (
                        pf.groupby(["date_label", "_primary_flag"])["count"]
                        .count().reset_index()
                        .rename(columns={"count": "件数", "_primary_flag": "起因フラグ"})
                        .sort_values("date_label")
                    )

                    # サマリー数値
                    total_inc  = len(pf)
                    total_vol  = int(pf["count"].sum())
                    nature_inc = pf["weather_flag"].apply(
                        lambda x: any(f in str(x).split("|") for f in ["天候", "樹木・倒木"])
                    ).sum()
                    w_pct      = f"{nature_inc/total_inc*100:.0f}%" if total_inc > 0 else "—"
                    top_cause  = (
                        pf[pf["raw_reason"].str.strip() != ""]["raw_reason"]
                        .value_counts().index[0]
                        if not pf[pf["raw_reason"].str.strip() != ""].empty else "—"
                    )

                    with sm_cols[p_idx % 4]:
                        # カードヘッダー
                        st.markdown(
                            f'<div style="display:flex; align-items:center; gap:6px;'
                            f' margin-bottom:4px;">'
                            f'<div style="width:10px;height:10px;border-radius:50%;'
                            f'background:{dot_color};flex-shrink:0;"></div>'
                            f'<span style="font-weight:700; font-size:0.88rem;">{pref}</span>'
                            f'</div>',
                            unsafe_allow_html=True,
                        )

                        if total_inc == 0:
                            st.markdown(
                                '<div style="font-size:0.75rem; color:#9ca3af;'
                                ' padding:8px; background:#f8fafc; border-radius:6px;'
                                ' text-align:center; margin-bottom:12px;">'
                                '該当データなし</div>',
                                unsafe_allow_html=True,
                            )
                        else:
                            # ミニチャート
                            fig_mini = px.bar(
                                mini_data, x="date_label", y="件数", color="起因フラグ",
                                color_discrete_map=_W_COLORS, barmode="stack",
                            )
                            fig_mini.update_layout(
                                height=130,
                                margin=dict(t=2, b=2, l=2, r=2),
                                showlegend=False,
                                plot_bgcolor="white", paper_bgcolor="white",
                            )
                            fig_mini.update_xaxes(showticklabels=False, showgrid=False,
                                                  zeroline=False)
                            fig_mini.update_yaxes(showticklabels=True, gridcolor="#f3f4f6",
                                                  tickformat=",d", nticks=3)
                            st.plotly_chart(fig_mini, use_container_width=True,
                                            key=f"mini_{pref}")

                            # 統計サマリー
                            st.markdown(
                                f'<div style="background:#f8fafc; border-radius:6px;'
                                f' padding:7px 10px; font-size:0.73rem; margin-bottom:14px;">'
                                f'<div style="display:grid; grid-template-columns:1fr 1fr;'
                                f' gap:3px;">'
                                f'<div><span style="color:#6b7280;">件数</span>'
                                f' <b>{total_inc:,}</b></div>'
                                f'<div><span style="color:#6b7280;">軒数</span>'
                                f' <b>{total_vol:,}</b></div>'
                                f'<div><span style="color:#6b7280;">自然起因</span>'
                                f' <b style="color:#0369a1;">'
                                f'{w_pct}</b></div>'
                                f'<div style="grid-column:span 2;'
                                f' color:#6b7280; margin-top:2px;">'
                                f'主因: <b style="color:#374151;">{top_cause[:16]}</b></div>'
                                f'</div></div>',
                                unsafe_allow_html=True,
                            )

                # 4列に7県なので最後の列に空白パディング（5〜7番目がない列）
                for pad in range(len(_TOHOKU_PREF_ORDER) % 4, 4 if len(_TOHOKU_PREF_ORDER) % 4 else 0):
                    with sm_cols[3 - pad]:
                        st.empty()

                # ── 都道府県 × 日付 ヒートマップ ───────────────────
                st.markdown('<div class="section-title">都道府県 × 発生日 停電件数ヒートマップ</div>',
                            unsafe_allow_html=True)
                heat_t = (
                    dft.groupby(["pref_name", "date_label"])["count"]
                    .count().reset_index()
                    .rename(columns={"count": "件数"})
                    .pivot(index="pref_name", columns="date_label", values="件数").fillna(0)
                    .reindex(_TOHOKU_PREF_ORDER)
                )
                if not heat_t.empty:
                    fig_ht = go.Figure(go.Heatmap(
                        z=heat_t.values,
                        x=heat_t.columns.tolist(),
                        y=heat_t.index.tolist(),
                        colorscale="YlOrRd",
                        hovertemplate="<b>%{y}</b>  %{x}<br>停電件数: %{z:.0f}件<extra></extra>",
                        colorbar=dict(title="停電件数"),
                    ))
                    fig_ht.update_layout(
                        height=320, margin=dict(t=10, b=10),
                        xaxis=dict(showgrid=False, tickangle=-45),
                        yaxis=dict(showgrid=False),
                    )
                    st.plotly_chart(fig_ht, use_container_width=True)

                # ── 詳細テーブル ─────────────────────────────────────
                st.markdown(
                    '<div class="section-title">停電記録一覧 ※列ヘッダーをクリックでソート</div>',
                    unsafe_allow_html=True,
                )
                # weather_flag 列が旧キャッシュで欠落している場合は補完
                if "weather_flag" not in dft.columns:
                    from scraper import _classify_weather as _cw
                    dft = dft.copy()
                    dft["weather_flag"] = dft["raw_reason"].apply(_cw)

                disp_t = (
                    dft[[
                        "date_label", "pref_name", "area_name",
                        "raw_reason", "weather_flag",
                        "count", "start_time", "recovery_time", "duration_h",
                    ]]
                    .rename(columns={
                        "date_label":   "発生日",
                        "pref_name":    "都道府県",
                        "area_name":    "停電地域",
                        "raw_reason":   "起因（原文）",
                        "weather_flag": "起因フラグ",
                        "count":        "停電軒数",
                        "start_time":   "発生時刻",
                        "recovery_time": "復旧時刻",
                        "duration_h":   "停電時間(h)",
                    })
                    .sort_values(["発生日", "都道府県"], ascending=[False, True])
                    .reset_index(drop=True)
                )

                st.markdown(
                    build_outage_table_html(disp_t),
                    unsafe_allow_html=True,
                )

                # データ取得元リンク
                st.markdown(
                    '<div style="font-size:0.75rem; color:#9ca3af; margin-top:8px;">'
                    'データ取得元: '
                    '<a href="https://nw.tohoku-epco.co.jp/teideninfo/rireki.html" '
                    'target="_blank" style="color:#3b82f6">東北電力ネットワーク 停電履歴ページ</a>'
                    '</div>',
                    unsafe_allow_html=True,
                )

    # ── 北海道電力NW ──────────────────────────────────────────
    with ct_hokkaido:
        render_company_detail(
            company_name="北海道電力ネットワーク",
            pref_order=_HOKKAIDO_PREF_ORDER,
            pref_colors=_HOKKAIDO_PREF_COLOR,
            rt_url="https://teiden-info.hepco.co.jp/",
            hist_url="https://teiden-info.hepco.co.jp/past00000000.html",
            df_rt=_df_rt_comp,
            df_hist=_df_hist_comp,
            key_prefix="hokkaido",
            n_hist_days="過去7日",
        )

    # ── 北陸電力送配電 ────────────────────────────────────────
    with ct_rikuden:
        render_company_detail(
            company_name="北陸電力送配電",
            pref_order=_RIKUDEN_PREF_ORDER,
            pref_colors=_RIKUDEN_PREF_COLOR,
            rt_url="https://www.rikuden.co.jp/nw/teiden/otj010.html",
            hist_url="https://www.rikuden.co.jp/nw/teiden/otj600.html",
            df_rt=_df_rt_comp,
            df_hist=_df_hist_comp,
            key_prefix="rikuden",
            n_hist_days="過去7日",
        )

    # ── 東京電力パワーグリッド ────────────────────────────────
    with ct_tepco:
        render_company_detail(
            company_name="東京電力パワーグリッド",
            pref_order=_TEPCO_PREF_ORDER,
            pref_colors=_TEPCO_PREF_COLOR,
            rt_url="https://teideninfo.tepco.co.jp/",
            hist_url="https://teideninfo.tepco.co.jp/day/history.html",
            df_rt=_df_rt_comp,
            df_hist=_df_hist_comp,
            key_prefix="tepco",
            n_hist_days="過去60日",
        )

    # ── 関西電力送配電 ────────────────────────────────────────
    with ct_kansai:
        render_company_detail(
            company_name="関西電力送配電",
            pref_order=_KANSAI_PREF_ORDER,
            pref_colors=_KANSAI_PREF_COLOR,
            rt_url="https://www.kansai-td.co.jp/teiden-info/index.php",
            hist_url="https://www.kansai-td.co.jp/teiden-info/index.php",
            df_rt=_df_rt_comp,
            df_hist=_df_hist_comp,
            key_prefix="kansai",
            n_hist_days="過去7日",
        )

    # ── 四国電力送配電 ────────────────────────────────────────
    with ct_shikoku:
        render_company_detail(
            company_name="四国電力送配電",
            pref_order=_SHIKOKU_PREF_ORDER,
            pref_colors=_SHIKOKU_PREF_COLOR,
            rt_url="https://www.yonden.co.jp/nw/teiden-info/index.html",
            hist_url="https://www.yonden.co.jp/nw/teiden-info/history.html",
            df_rt=_df_rt_comp,
            df_hist=_df_hist_comp,
            key_prefix="shikoku",
            n_hist_days="過去31日（件数のみ・起因なし）",
        )

    # ── 中国電力ネットワーク ──────────────────────────────────
    with ct_chugoku:
        render_company_detail(
            company_name="中国電力ネットワーク",
            pref_order=_CHUGOKU_PREF_ORDER,
            pref_colors=_CHUGOKU_PREF_COLOR,
            rt_url="https://www.teideninfo.energia.co.jp/",
            hist_url="https://www.teideninfo.energia.co.jp/LWC30040/index",
            df_rt=_df_rt_comp,
            df_hist=_df_hist_comp,
            key_prefix="chugoku",
            n_hist_days="過去7日",
        )

    # ── 中部電力パワーグリッド ────────────────────────────────
    with ct_chubu:
        render_company_detail(
            company_name="中部電力パワーグリッド",
            pref_order=_CHUBU_PREF_ORDER,
            pref_colors=_CHUBU_PREF_COLOR,
            rt_url="https://teiden.powergrid.chuden.co.jp/p/index.html",
            hist_url="https://teiden.powergrid.chuden.co.jp/p/index.html",
            df_rt=_df_rt_comp,
            df_hist=_df_hist_comp,
            key_prefix="chubu",
            n_hist_days="（履歴・起因情報は非公開）",
        )

    # ── 九州電力送配電 ────────────────────────────────────────
    with ct_kyushu:
        render_company_detail(
            company_name="九州電力送配電",
            pref_order=_KYUSHU_PREF_ORDER,
            pref_colors=_KYUSHU_PREF_COLOR,
            rt_url="https://www.kyuden.co.jp/td_teiden/kyushu.html",
            hist_url="https://www.kyuden.co.jp/td_teiden/",
            df_rt=_df_rt_comp,
            df_hist=_df_hist_comp,
            key_prefix="kyushu",
            n_hist_days="過去7日",
        )

    # ── 沖縄電力 ──────────────────────────────────────────────
    with ct_okinawa:
        render_company_detail(
            company_name="沖縄電力",
            pref_order=_OKINAWA_PREF_ORDER,
            pref_colors=_OKINAWA_PREF_COLOR,
            rt_url="https://www.okidenmail.jp/bosai/info/index.html",
            hist_url="https://www.okidenmail.jp/bosai/info/index.html",
            df_rt=_df_rt_comp,
            df_hist=_df_hist_comp,
            key_prefix="okinawa",
            n_hist_days="過去数日（JS動的レンダリングのため取得制限あり）",
        )


# ═══════════════════════════════════════════════════
# タブ4: 過去1年の停電実績（シミュレーション）
# ═══════════════════════════════════════════════════
with tab_hist:
    st.info(
        "⚠️ 過去の停電実績は公開APIが存在しないため、**シミュレーションデータ**を表示しています。"
        " 実際の過去データは各電力会社の公式サイト/年次報告書をご参照ください。"
    )

    df_hist = get_historical_data()

    with st.expander("🔍 フィルター設定", expanded=False):
        f1, f2 = st.columns(2)
        with f1:
            sel_region = st.selectbox("地域", ["全地域"] + sorted(df_hist["region"].unique().tolist()), key="hist_region")
        with f2:
            sel_company = st.selectbox("電力会社", ["全電力会社"] + sorted(df_hist["company"].unique().tolist()), key="hist_company")

    df_f = df_hist.copy()
    if sel_region  != "全地域":     df_f = df_f[df_f["region"]  == sel_region]
    if sel_company != "全電力会社": df_f = df_f[df_f["company"] == sel_company]

    k1, k2, k3, k4 = st.columns(4)
    with k1: st.metric("年間停電件数", f"{df_f['incidents'].sum():,} 件")
    with k2: st.metric("年間停電軒数", f"{df_f['affected_customers'].sum():,} 軒")
    with k3: st.metric("年間停電時間", f"{df_f['total_outage_hours'].sum():,.0f} 時間")
    with k4:
        top_p = df_f.groupby("prefecture")["affected_customers"].sum().idxmax() if not df_f.empty else "—"
        st.metric("最多影響都道府県", top_p)

    st.markdown("")

    # 月別トレンド
    st.markdown('<div class="section-title">月別 停電件数・停電軒数の推移</div>', unsafe_allow_html=True)
    monthly = df_f.groupby("month_label").agg(
        incidents=("incidents", "sum"), affected_customers=("affected_customers", "sum"),
    ).reset_index()
    fig_trend = make_subplots(specs=[[{"secondary_y": True}]])
    fig_trend.add_trace(go.Bar(x=monthly["month_label"], y=monthly["affected_customers"],
                               name="停電軒数", marker_color="#93c5fd", opacity=0.75), secondary_y=False)
    fig_trend.add_trace(go.Scatter(x=monthly["month_label"], y=monthly["incidents"],
                                   name="停電件数", mode="lines+markers",
                                   line=dict(color="#ef4444", width=2.5), marker=dict(size=7)), secondary_y=True)
    fig_trend.update_layout(height=360, hovermode="x unified", legend=dict(orientation="h", y=1.08),
                            margin=dict(t=20, b=20), plot_bgcolor="white", paper_bgcolor="white")
    fig_trend.update_xaxes(showgrid=False)
    fig_trend.update_yaxes(title_text="停電軒数（軒）", secondary_y=False, gridcolor="#f3f4f6", tickformat=",")
    fig_trend.update_yaxes(title_text="停電件数（件）", secondary_y=True, showgrid=False)
    st.plotly_chart(fig_trend, use_container_width=True)

    # 地域別・電力会社別
    c1, c2 = st.columns(2)
    with c1:
        st.markdown('<div class="section-title">地域別 累計停電軒数</div>', unsafe_allow_html=True)
        ra = df_f.groupby("region")["affected_customers"].sum().sort_values().reset_index()
        fig_r = px.bar(ra, x="affected_customers", y="region", orientation="h",
                       color="affected_customers", color_continuous_scale="Blues",
                       labels={"affected_customers": "停電軒数", "region": "地域"})
        fig_r.update_layout(height=360, margin=dict(t=10, b=10), coloraxis_showscale=False,
                            plot_bgcolor="white", paper_bgcolor="white")
        fig_r.update_xaxes(gridcolor="#f3f4f6", tickformat=",")
        fig_r.update_yaxes(showgrid=False)
        st.plotly_chart(fig_r, use_container_width=True)
    with c2:
        st.markdown('<div class="section-title">電力会社別 停電件数・停電軒数</div>', unsafe_allow_html=True)
        ca = (df_f.groupby("company")
              .agg(incidents=("incidents", "sum"), customers=("affected_customers", "sum"))
              .reset_index().sort_values("customers", ascending=False))
        fig_co = make_subplots(specs=[[{"secondary_y": True}]])
        fig_co.add_trace(go.Bar(x=ca["company"], y=ca["customers"], name="停電軒数",
                                marker_color="#6ee7b7", opacity=0.8), secondary_y=False)
        fig_co.add_trace(go.Scatter(x=ca["company"], y=ca["incidents"], name="停電件数",
                                    mode="markers+lines", marker=dict(size=8, color="#7c3aed"),
                                    line=dict(color="#7c3aed", width=2)), secondary_y=True)
        fig_co.update_layout(height=360, hovermode="x unified", legend=dict(orientation="h", y=1.08),
                             margin=dict(t=20, b=20), plot_bgcolor="white", paper_bgcolor="white")
        fig_co.update_xaxes(showgrid=False)
        fig_co.update_yaxes(title_text="停電軒数", secondary_y=False, gridcolor="#f3f4f6", tickformat=",")
        fig_co.update_yaxes(title_text="停電件数", secondary_y=True, showgrid=False)
        st.plotly_chart(fig_co, use_container_width=True)

    # 都道府県 × 月 ヒートマップ
    st.markdown('<div class="section-title">都道府県 × 月別 停電軒数ヒートマップ</div>', unsafe_allow_html=True)
    heat = (df_f.groupby(["prefecture", "month_label"])["affected_customers"]
            .sum().reset_index()
            .pivot(index="prefecture", columns="month_label", values="affected_customers").fillna(0))
    if not heat.empty:
        fig_heat = go.Figure(go.Heatmap(
            z=heat.values, x=heat.columns.tolist(), y=heat.index.tolist(),
            colorscale="YlOrRd",
            hovertemplate="<b>%{y}</b><br>%{x}<br>停電軒数: %{z:,}軒<extra></extra>",
            colorbar=dict(title="停電軒数", tickformat=","),
        ))
        fig_heat.update_layout(height=820, margin=dict(t=10, b=10, l=10, r=10),
                               xaxis=dict(showgrid=False), yaxis=dict(showgrid=False, autorange="reversed"))
        st.plotly_chart(fig_heat, use_container_width=True)

# ─── フッター ─────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    "<p style='text-align:center; color:#9ca3af; font-size:0.78rem;'>"
    "リアルタイム・停電履歴データ提供元: "
    "<a href='https://nw.tohoku-epco.co.jp/teideninfo/rireki.html' target='_blank' style='color:#3b82f6'>東北電力ネットワーク</a> / "
    "<a href='https://www.kansai-td.co.jp/teiden-info/index.php' target='_blank' style='color:#3b82f6'>関西電力送配電</a> / "
    "<a href='https://www.yonden.co.jp/nw/teiden-info/history.html' target='_blank' style='color:#3b82f6'>四国電力送配電</a>"
    "（各社公式ホームページ）<br>"
    "過去1年実績はシミュレーションデータです。"
    " 参考: <a href='https://typhoon.yahoo.co.jp/weather/poweroutage/' target='_blank' style='color:#3b82f6'>"
    "Yahoo!天気・災害 停電情報</a>"
    " | <a href='https://www.fepc.or.jp/sp/bousai/link.html' target='_blank' style='color:#3b82f6'>"
    "電気事業連合会</a>"
    "</p>",
    unsafe_allow_html=True,
)
