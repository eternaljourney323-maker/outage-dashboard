import html as _html
import xml.etree.ElementTree as ET
import datetime as _dt
import logging
import math as _math
from typing import Optional, List
import streamlit as st
import streamlit.components.v1 as _components
import json as _json
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as _pio
from plotly.subplots import make_subplots
import pandas as pd
from datetime import datetime
from urllib.parse import quote
import requests as _req

logger = logging.getLogger(__name__)

from modules.data_generator import CATEGORY_COLOR
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
    initial_sidebar_state="expanded",
)

# ─── 言語設定（Chrome翻訳プロンプト抑制）──────────────────────
st.markdown(
    '<script>document.documentElement.lang = "ja";</script>',
    unsafe_allow_html=True,
)

# ─── CSS ──────────────────────────────────────────────────────
import pathlib as _pl
_css_path = _pl.Path(__file__).parent / "styles" / "main.css"
st.markdown(f"<style>{_css_path.read_text(encoding='utf-8')}</style>",
            unsafe_allow_html=True)

# ─── 色定義（停電レベル → 色）─────────────────────────────────
LEVEL_COLORS = {
    "停電なし":     "#16a34a",
    "〜100軒":      "#fde68a",
    "〜1,000軒":    "#f59e0b",
    "〜10,000軒":   "#ea580c",
    "10,000軒以上": "#dc2626",
    "データ未取得": "#94a3b8",
}

# ─── 電力会社別 都道府県リスト ────────────────────────────────
_COMPANY_PREFS: dict[str, list] = {
    "北海道電力ネットワーク": ["北海道"],
    "東北電力ネットワーク":   ["青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県", "新潟県"],
    "北陸電力送配電":         ["富山県", "石川県", "福井県"],
    "東京電力パワーグリッド": ["茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県", "山梨県", "静岡県"],
    "中部電力パワーグリッド": ["愛知県", "三重県", "岐阜県", "静岡県", "長野県"],
    "関西電力送配電":         ["滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県"],
    "中国電力ネットワーク":   ["鳥取県", "島根県", "岡山県", "広島県", "山口県"],
    "四国電力送配電":         ["香川県", "愛媛県", "徳島県", "高知県"],
    "九州電力送配電":         ["福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県"],
    "沖縄電力":               ["沖縄県"],
}

_LEVEL_SEVERITY = ["10,000軒以上", "〜10,000軒", "〜1,000軒", "〜100軒", "停電なし", "データ未取得"]

_GRID_AREAS = {
    "北海道電力ネットワーク": "hokkaido",
    "東北電力ネットワーク":   "tohoku",
    "北陸電力送配電":         "rikuden",
    "東京電力パワーグリッド": "kanto",
    "中部電力パワーグリッド": "chubu",
    "関西電力送配電":         "kansai",
    "中国電力ネットワーク":   "chugoku",
    "四国電力送配電":         "shikoku",
    "九州電力送配電":         "kyushu",
    "沖縄電力":               "okinawa",
}

_GRID_TEMPLATE = (
    "'. . . hokkaido' "
    "'. tohoku tohoku tohoku' "
    "'rikuden chubu kanto kanto' "
    "'kansai chubu kanto kanto' "
    "'chugoku chugoku shikoku .' "
    "'kyushu kyushu . .' "
    "'okinawa . . .'"
)


def build_company_map_html(df: pd.DataFrame) -> str:
    """電力会社別・簡易日本地図配置のクリッカブルHTMLカード"""

    def _short(name: str) -> str:
        return name[:-1] if name.endswith(("県", "府", "都")) else name

    cards = ""
    for company, grid_area in _GRID_AREAS.items():
        url     = _COMPANY_URLS.get(company, "#")
        prefs   = _COMPANY_PREFS.get(company, [])
        comp_df = df[df["data_source"] == company]

        levels   = comp_df["outage_level"].tolist() if not comp_df.empty else []
        worst    = next((lvl for lvl in _LEVEL_SEVERITY if lvl in levels), "データ未取得")
        card_bg  = LEVEL_COLORS.get(worst, "#cbd5e1")
        card_txt = "#fff" if worst in ["10,000軒以上", "〜10,000軒"] else "#1e293b"

        short = (
            company
            .replace("電力ネットワーク", "電力NW")
            .replace("パワーグリッド", "PG")
            .replace("送配電", "")
        )

        pref_boxes = ""
        for p in prefs:
            row     = df[df["prefecture"] == p]
            p_level = str(row.iloc[0]["outage_level"]) if not row.empty else "データ未取得"
            p_bg    = LEVEL_COLORS.get(p_level, "#cbd5e1")
            p_cnt   = int(row.iloc[0]["affected_customers"]) \
                      if not row.empty and row.iloc[0]["data_status"] == "取得済み" else 0
            p_txt   = "#fff" if p_level in ["10,000軒以上", "〜10,000軒"] else "#1e293b"
            cnt_div = (
                f'<div style="font-size:0.63rem;font-weight:700;color:{p_txt};'
                f'line-height:1.1;">{p_cnt:,}</div>'
            ) if p_cnt > 0 else ""
            pref_boxes += (
                f'<div style="background:{p_bg};border-radius:5px;padding:4px 2px;'
                f'text-align:center;border:1.5px solid rgba(255,255,255,0.55);'
                f'min-width:0;overflow:hidden;">'
                f'<div style="font-size:0.65rem;font-weight:600;color:{p_txt};'
                f'white-space:nowrap;">{_short(p)}</div>'
                f'{cnt_div}'
                f'</div>'
            )

        cards += (
            f'<a href="{url}" target="_blank" rel="noopener noreferrer"'
            f' style="grid-area:{grid_area};text-decoration:none;'
            f'background:{card_bg};border-radius:8px;padding:10px 12px;display:block;'
            f'box-shadow:0 1px 2px rgba(15,23,42,0.08);'
            f'border:1px solid rgba(23,32,51,0.12);"'
            f' onmouseover="this.style.transform=\'scale(1.01)\';'
            f'this.style.boxShadow=\'0 8px 24px rgba(15,23,42,0.14)\'"'
            f' onmouseout="this.style.transform=\'scale(1)\';'
            f'this.style.boxShadow=\'0 1px 2px rgba(15,23,42,0.08)\'">'
            f'<div style="font-size:0.72rem;font-weight:700;color:{card_txt};'
            f'margin-bottom:6px;">{short}</div>'
            f'<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(46px,1fr));'
            f'gap:3px;">{pref_boxes}</div>'
            f'</a>'
        )

    return (
        f'<div style="display:grid;'
        f'grid-template-columns:18% 24% 32% 26%;'
        f'grid-template-rows:repeat(7,auto);'
        f'grid-template-areas:{_GRID_TEMPLATE};'
        f'gap:8px;padding:14px;'
        f'background:#ffffff;border:1px solid #dbe3ee;border-radius:8px;'
        f'box-shadow:0 1px 2px rgba(15,23,42,0.04),0 8px 24px rgba(15,23,42,0.06);">'
        f'{cards}</div>'
    )


def pref_list_wide_html(df: pd.DataFrame) -> str:
    """ワイド表示用 停電状況リスト"""
    active  = df[df["affected_customers"] > 0].sort_values("affected_customers", ascending=False)
    no_data = df[df["data_status"] == "取得不可"]

    if active.empty and no_data.empty:
        return (
            '<div style="padding:16px; text-align:center; color:#166534; font-weight:700;'
            ' background:#ffffff; border:1px solid #dbe3ee; border-radius:8px;'
            ' box-shadow:0 1px 2px rgba(15,23,42,0.04),0 8px 24px rgba(15,23,42,0.06);">'
            '現在、停電は確認されていません</div>'
        )

    html = (
        '<div style="background:#ffffff; border:1px solid #dbe3ee; border-radius:8px; padding:12px 14px;'
        ' box-shadow:0 1px 2px rgba(15,23,42,0.04),0 8px 24px rgba(15,23,42,0.06);">'
    )

    if not active.empty:
        html += (
            '<div style="font-size:0.72rem; font-weight:700; color:#dc2626;'
            ' margin-bottom:8px; display:flex; align-items:center; gap:6px;">'
            '<span style="display:inline-block;width:8px;height:8px;border-radius:50%;'
            'background:#dc2626;"></span>停電中'
            '<span style="font-size:0.68rem; font-weight:400; color:#94a3b8;">'
            '（クリックで各社サイトへ）</span></div>'
            '<div style="display:flex; flex-wrap:wrap; gap:8px; margin-bottom:10px;">'
        )
        for _, r in active.iterrows():
            bg    = LEVEL_COLORS.get(r["outage_level"], "#f59e0b")
            txt_c = "#fff" if r["outage_level"] in ["10,000軒以上", "〜10,000軒"] else "#1e293b"
            url   = _COMPANY_URLS.get(str(r.get("data_source", "")), "#")
            html += (
                f'<a href="{url}" target="_blank" rel="noopener noreferrer"'
                f' style="background:{bg};border-radius:8px;padding:8px 14px;'
                f'min-width:120px;border:1px solid rgba(23,32,51,0.08);'
                f'text-decoration:none;display:block;'
                f'transition:transform 0.15s,box-shadow 0.15s;"'
                f' onmouseover="this.style.transform=\'scale(1.04)\';'
                f'this.style.boxShadow=\'0 4px 14px rgba(0,0,0,0.18)\'"'
                f' onmouseout="this.style.transform=\'scale(1)\';'
                f'this.style.boxShadow=\'0 1px 6px rgba(0,0,0,0.1)\'">'
                f'<div style="font-size:0.82rem;font-weight:700;color:{txt_c};">'
                f'{r["prefecture"]}</div>'
                f'<div style="font-size:1.1rem;font-weight:700;color:{txt_c};">'
                f'{r["affected_customers"]:,}'
                f'<span style="font-size:0.7rem;font-weight:400;"> 軒</span></div>'
                f'<div style="font-size:0.68rem;color:{txt_c};opacity:0.8;">'
                f'{r["outage_level"]}</div>'
                f'</a>'
            )
        html += '</div>'

    if not no_data.empty:
        html += (
            '<div style="font-size:0.7rem; font-weight:700; color:#64748b;'
            ' margin-bottom:5px;">○ データ未取得</div>'
            '<div style="display:flex; flex-wrap:wrap; gap:5px;">'
        )
        for _, r in no_data.sort_values("prefecture").iterrows():
            url = _COMPANY_URLS.get(str(r.get("data_source", "")), "#")
            html += (
                f'<a href="{url}" target="_blank" rel="noopener noreferrer"'
                f' style="background:#f1f5f9;border-radius:5px;padding:3px 8px;'
                f'font-size:0.7rem;color:#64748b;border:1px solid #e2e8f0;'
                f'text-decoration:none;">'
                f'{r["prefecture"]}</a>'
            )
        html += '</div>'

    html += '</div>'
    return html


_PREF_TILE_POS = {
    "北海道": (10, 1), "青森県": (10, 2), "秋田県": (10, 3), "岩手県": (11, 3),
    "山形県": (10, 4), "宮城県": (11, 4), "福島県": (10, 5),
    "群馬県": (10, 6), "栃木県": (11, 6), "埼玉県": (10, 7), "茨城県": (11, 7),
    "東京都": (10, 8), "千葉県": (11, 8), "神奈川県": (10, 9),
    "新潟県": (9, 6), "富山県": (8, 6), "石川県": (7, 6), "福井県": (7, 7),
    "長野県": (9, 7), "山梨県": (9, 8), "静岡県": (9, 9),
    "岐阜県": (8, 7), "愛知県": (8, 8), "三重県": (8, 9),
    "滋賀県": (7, 8), "京都府": (6, 8), "大阪府": (6, 9), "奈良県": (7, 9),
    "和歌山県": (6, 10), "兵庫県": (5, 8),
    "鳥取県": (4, 8), "島根県": (3, 8), "岡山県": (4, 9), "広島県": (3, 9),
    "山口県": (2, 8),
    "香川県": (5, 10), "徳島県": (5, 11), "愛媛県": (4, 10), "高知県": (4, 11),
    "福岡県": (2, 10), "佐賀県": (1, 10), "長崎県": (1, 11), "熊本県": (2, 11),
    "大分県": (3, 10), "宮崎県": (3, 11), "鹿児島県": (2, 12),
    "沖縄県": (1, 13),
}

_MAP_COMPANY_STYLES = {
    "北海道電力ネットワーク": ("#bfdbfe", "#3b82f6", "#1e3a8a", "北海道電力"),      # 青
    "東北電力ネットワーク":   ("#ecfccb", "#84cc16", "#365314", "東北電力"),          # ライム
    "東京電力パワーグリッド": ("#bbf7d0", "#16a34a", "#14532d", "東京電力パワーグリッド"),  # 緑
    "中部電力パワーグリッド": ("#fed7aa", "#ea580c", "#7c2d12", "中部電力パワーグリッド"),  # 橙
    "北陸電力送配電":         ("#c7d2fe", "#4f46e5", "#312e81", "北陸電力送配電"),    # インディゴ
    "関西電力送配電":         ("#fef08a", "#ca8a04", "#713f12", "関西電力送配電"),    # 黄
    "中国電力ネットワーク":   ("#fbcfe8", "#db2777", "#831843", "中国電力ネットワーク"),  # ピンク
    "四国電力送配電":         ("#e9d5ff", "#9333ea", "#581c87", "四国電力送配電"),    # 紫
    "九州電力送配電":         ("#fca5a5", "#dc2626", "#7f1d1d", "九州電力送配電"),    # 赤
    "沖縄電力":               ("#a5f3fc", "#0891b2", "#164e63", "沖縄電力"),          # シアン
}

_MAP_COMPANY_ORDER = [
    "北海道電力ネットワーク",
    "東北電力ネットワーク",
    "東京電力パワーグリッド",
    "中部電力パワーグリッド",
    "北陸電力送配電",
    "関西電力送配電",
    "中国電力ネットワーク",
    "四国電力送配電",
    "九州電力送配電",
    "沖縄電力",
]

_PREF_TO_MAP_COMPANY = {
    pref: company
    for company in _MAP_COMPANY_ORDER
    for pref in _COMPANY_PREFS.get(company, [])
}
_PREF_TO_MAP_COMPANY["静岡県"] = "中部電力パワーグリッド"

_PREF_TILE_SPAN = {
    "北海道": (2, 1),
    "青森県": (2, 1),
    "福島県": (2, 1),
    "神奈川県": (1, 2),
    "和歌山県": (2, 1),
    "鹿児島県": (2, 1),
}

# 各電力会社エリアラベルの配置位置（グリッド内の空きセル）
_COMPANY_LABEL_POS: dict[str, tuple[int, int]] = {
    "北海道電力ネットワーク": (9, 1),   # 北海道(10-11, 1)の左
    "東北電力ネットワーク":   (8, 3),   # 東北エリア中央左
    "東京電力パワーグリッド": (8, 5),   # 東京PGエリア上
    "北陸電力送配電":         (6, 6),   # 石川(7,6)・富山(8,6)の左
    "中部電力パワーグリッド": (6, 7),   # 岐阜(8,7)・長野(9,7)の左
    "関西電力送配電":         (4, 7),   # 兵庫(5,8)上左
    "中国電力ネットワーク":   (1, 8),   # 山口/島根行の左端
    "四国電力送配電":         (4, 12),  # 四国タイル(10-11行)下
    "九州電力送配電":         (1, 9),   # 佐賀(1,10)の上
    "沖縄電力":               (2, 13),  # 沖縄(1,13)の右
}

_COMPANY_LABEL_TEXT: dict[str, str] = {
    "北海道電力ネットワーク": "北海道<br>電力NW",
    "東北電力ネットワーク":   "東北<br>電力NW",
    "東京電力パワーグリッド": "東京<br>電力PG",
    "北陸電力送配電":         "北陸<br>電力",
    "中部電力パワーグリッド": "中部<br>電力PG",
    "関西電力送配電":         "関西<br>電力",
    "中国電力ネットワーク":   "中国<br>電力NW",
    "四国電力送配電":         "四国<br>電力",
    "九州電力送配電":         "九州<br>電力",
    "沖縄電力":               "沖縄<br>電力",
}

_JAPAN_GEOJSON_URL = (
    "https://raw.githubusercontent.com/dataofjapan/land/master/japan.geojson"
)

_JMA_NOWC_TARGETS_URL = (
    "https://www.jma.go.jp/bosai/jmatile/data/nowc/targetTimes_N1.json"
)
_JMA_TYPHOON_TARGETS_URL = (
    "https://www.jma.go.jp/bosai/typhoon/data/targetTc.json"
)
_JMA_TYPHOON_DATA_BASE = "https://www.jma.go.jp/bosai/typhoon/data"
_JMA_BASE_TILE_URL = "https://www.jma.go.jp/tile/jma/base/{z}/{x}/{y}.png"


@st.cache_data(ttl=300, show_spinner=False)
def load_jma_radar_layer() -> Optional[dict]:
    """気象庁高解像度降水ナウキャストの最新タイル情報。"""
    try:
        response = _req.get(_JMA_NOWC_TARGETS_URL, timeout=12)
        response.raise_for_status()
        targets = response.json()
        latest = next(
            item for item in targets
            if "hrpns" in item.get("elements", [])
        )
        basetime = str(latest["basetime"])
        validtime = str(latest["validtime"])
        observed_utc = _dt.datetime.strptime(validtime, "%Y%m%d%H%M%S").replace(
            tzinfo=_dt.timezone.utc
        )
        observed_jst = observed_utc.astimezone(
            _dt.timezone(_dt.timedelta(hours=9))
        )
        return {
            "tile_url": (
                "https://www.jma.go.jp/bosai/jmatile/data/nowc/"
                f"{basetime}/none/{validtime}/surf/hrpns/"
                "{z}/{x}/{y}.png"
            ),
            "observed_at": observed_jst.strftime("%m/%d %H:%M"),
        }
    except Exception as exc:
        logger.warning("気象庁雨雲レーダー取得失敗: %s", exc)
        return None


@st.cache_data(ttl=300, show_spinner=False)
def load_jma_lightning_layer() -> Optional[dict]:
    """気象庁雷ナウキャストの最新タイル情報。"""
    try:
        response = _req.get(_JMA_NOWC_TARGETS_URL, timeout=12)
        response.raise_for_status()
        targets = response.json()
        latest = next(
            item for item in targets
            if "thns" in item.get("elements", [])
        )
        basetime = str(latest["basetime"])
        validtime = str(latest["validtime"])
        observed_utc = _dt.datetime.strptime(validtime, "%Y%m%d%H%M%S").replace(
            tzinfo=_dt.timezone.utc
        )
        observed_jst = observed_utc.astimezone(
            _dt.timezone(_dt.timedelta(hours=9))
        )
        return {
            "tile_url": (
                "https://www.jma.go.jp/bosai/jmatile/data/nowc/"
                f"{basetime}/none/{validtime}/surf/thns/"
                "{z}/{x}/{y}.png"
            ),
            "observed_at": observed_jst.strftime("%m/%d %H:%M"),
        }
    except Exception as exc:
        logger.warning("気象庁雷ナウキャスト取得失敗: %s", exc)
        return None


def _normalize_longitude(value: float) -> float:
    """0〜360度系の経度をPlotly用の-180〜180度系へ変換。"""
    return value - 360 if value > 180 else value


def _normalize_typhoon_forecast(
    target: dict,
    forecast: list[dict],
) -> Optional[dict]:
    """気象庁の台風進路JSONを地図表示用に正規化。"""
    title = next((row for row in forecast if row.get("part") == "title"), {})
    points = [row for row in forecast if isinstance(row.get("center"), list)]
    if not points:
        return None

    current = next((row for row in points if row.get("advancedHours") == 0), points[0])
    track = current.get("track", {})
    history_raw = track.get("preTyphoon", []) + track.get("typhoon", [])
    history: list[list[float]] = []
    for coords in history_raw:
        if not isinstance(coords, list) or len(coords) < 2:
            continue
        normalized = [float(coords[0]), _normalize_longitude(float(coords[1]))]
        if not history or history[-1] != normalized:
            history.append(normalized)

    forecast_points = []
    for row in points:
        center = row.get("center", [])
        if len(center) < 2:
            continue
        validtime = row.get("validtime", {}).get("JST", "")
        try:
            valid_label = _dt.datetime.fromisoformat(validtime).strftime("%m/%d %H:%M")
        except (TypeError, ValueError):
            valid_label = validtime or "時刻不明"
        probability = row.get("probabilityCircle") or {}
        forecast_points.append({
            "lat": float(center[0]),
            "lon": _normalize_longitude(float(center[1])),
            "hours": int(row.get("advancedHours", 0)),
            "valid_at": valid_label,
            "radius_m": float(probability.get("radius", 0) or 0),
        })

    name = title.get("name", {}).get("jp") or ""
    number = str(title.get("typhoonNumber") or target.get("typhoonNumber") or "")
    category = str(target.get("category") or "")
    if number.isdigit():
        display_name = f"台風第{int(number[-2:])}号"
    elif category == "TD":
        display_name = "熱帯低気圧"
    else:
        display_name = "熱帯低気圧"
    if name:
        display_name += f" {name}"

    return {
        "id": str(target.get("tropicalCyclone", "")),
        "name": display_name,
        "history": history,
        "forecast": forecast_points,
    }


@st.cache_data(ttl=600, show_spinner=False)
def load_jma_typhoon_tracks() -> Optional[list[dict]]:
    """気象庁が発表中の台風・熱帯低気圧の進路。"""
    try:
        response = _req.get(_JMA_TYPHOON_TARGETS_URL, timeout=12)
        response.raise_for_status()
        targets = response.json()
    except Exception as exc:
        logger.warning("気象庁台風一覧取得失敗: %s", exc)
        return None

    tracks = []
    for target in targets:
        cyclone_id = str(target.get("tropicalCyclone", ""))
        if not cyclone_id:
            continue
        try:
            response = _req.get(
                f"{_JMA_TYPHOON_DATA_BASE}/{cyclone_id}/forecast.json",
                timeout=12,
            )
            response.raise_for_status()
            normalized = _normalize_typhoon_forecast(target, response.json())
            if normalized:
                tracks.append(normalized)
        except Exception as exc:
            logger.warning("気象庁台風進路取得失敗 (%s): %s", cyclone_id, exc)
    return tracks

_MAP_LEVEL_NUM = {
    "データ未取得":   0,
    "停電なし":       1,
    "〜100軒":        2,
    "〜1,000軒":      3,
    "〜10,000軒":     4,
    "10,000軒以上":   5,
}

_MAP_COLORSCALE = [
    [0.000, "#94a3b8"], [0.166, "#94a3b8"],
    [0.167, "#d1fae5"], [0.333, "#d1fae5"],
    [0.334, "#fde68a"], [0.500, "#fde68a"],
    [0.501, "#fbbf24"], [0.667, "#fbbf24"],
    [0.668, "#ea580c"], [0.833, "#ea580c"],
    [0.834, "#dc2626"], [1.000, "#dc2626"],
]


@st.cache_data(ttl=86400 * 7, show_spinner=False)
def _fetch_japan_geojson():
    try:
        r = _req.get(_JAPAN_GEOJSON_URL, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        logger.warning("Japan GeoJSON 取得失敗: %s", exc)
        return None


@st.cache_data(ttl=86400 * 30, show_spinner=False)
def _compute_pref_centroids() -> dict:
    """都道府県名 → (lon, lat) セントロイドの辞書（キャッシュ）"""
    geojson = _fetch_japan_geojson()
    if geojson is None:
        return {}

    def _bbox_area(poly):
        ring = poly[0]
        xs = [c[0] for c in ring]
        ys = [c[1] for c in ring]
        return (max(xs) - min(xs)) * (max(ys) - min(ys))

    result = {}
    for feature in geojson["features"]:
        name = feature["properties"]["nam_ja"]
        geom = feature["geometry"]
        if name in _PREF_LABEL_POS_OVERRIDE:
            result[name] = _PREF_LABEL_POS_OVERRIDE[name]
        elif geom["type"] == "Polygon":
            coords = geom["coordinates"][0]
            if coords:
                result[name] = (
                    sum(c[0] for c in coords) / len(coords),
                    sum(c[1] for c in coords) / len(coords),
                )
        elif geom["type"] == "MultiPolygon":
            coords = max(geom["coordinates"], key=_bbox_area)[0]
            if coords:
                result[name] = (
                    sum(c[0] for c in coords) / len(coords),
                    sum(c[1] for c in coords) / len(coords),
                )
    return result


def _pref_short_label(name: str) -> str:
    """都道府県名を地図ラベル用に短縮（北海道・京都など誤切りを防ぐ）"""
    _FIXED = {
        "北海道": "北海道",
        "東京都": "東京",
        "京都府": "京都",
        "大阪府": "大阪",
    }
    if name in _FIXED:
        return _FIXED[name]
    if name.endswith("県"):
        return name[:-1]
    return name


# ラベル位置の手動補正（重なり・はみ出し対策）
_PREF_LABEL_POS_OVERRIDE = {
    "北海道":  (142.8, 43.4),
    "東京都":  (139.4, 35.9),
    "神奈川県": (139.4, 35.4),
    "埼玉県":  (139.3, 36.1),
    "千葉県":  (140.2, 35.6),
    "京都府":  (135.5, 35.2),
}


def build_japan_map_fig(df: pd.DataFrame):
    """電力会社エリア別色分け日本地図（停電エリア点滅・数字のみ表示）"""
    geojson = _fetch_japan_geojson()
    if geojson is None:
        return None

    # 電力会社インデックス → 離散カラースケール（trace 0）
    company_to_idx = {c: i for i, c in enumerate(_MAP_COMPANY_ORDER)}
    company_colors = [_MAP_COMPANY_STYLES[c][0] for c in _MAP_COMPANY_ORDER]
    n = len(company_colors)
    eps = 1e-5
    colorscale = []
    for i, color in enumerate(company_colors):
        t0 = i / n
        t1 = (i + 1) / n
        colorscale.append([t0, color])
        if i < n - 1:
            colorscale.append([t1 - eps, color])
    colorscale.append([1.0, company_colors[-1]])

    df_map = df.copy()
    df_map["company_idx"] = df_map["data_source"].map(company_to_idx).fillna(0).astype(int)
    df_map["hover"] = df_map.apply(
        lambda r: (
            f"<b>{r['prefecture']}</b><br>{r['data_source']}<br>{r['outage_level']}"
            + (f"<br><b>{int(r['affected_customers']):,}軒</b>"
               if r.get("data_status") == "取得済み" and r["affected_customers"] > 0
               else "")
        ),
        axis=1,
    )

    # trace 0: ベースコロプレス（全都道府県・会社色）
    choropleth_base = go.Choropleth(
        geojson=geojson,
        featureidkey="properties.nam_ja",
        locations=df_map["prefecture"].tolist(),
        z=df_map["company_idx"].tolist(),
        colorscale=colorscale,
        zmin=0, zmax=n,
        showscale=False,
        marker_line_color="white",
        marker_line_width=0.8,
        hovertext=df_map["hover"].tolist(),
        hoverinfo="text",
    )

    # セントロイド算出
    def _bbox_area(poly):
        ring = poly[0]
        xs = [c[0] for c in ring]
        ys = [c[1] for c in ring]
        return (max(xs) - min(xs)) * (max(ys) - min(ys))

    centroids = {}
    for feature in geojson["features"]:
        name = feature["properties"]["nam_ja"]
        geom = feature["geometry"]
        if name in _PREF_LABEL_POS_OVERRIDE:
            centroids[name] = _PREF_LABEL_POS_OVERRIDE[name]
        else:
            if geom["type"] == "Polygon":
                coords = geom["coordinates"][0]
            elif geom["type"] == "MultiPolygon":
                coords = max(geom["coordinates"], key=_bbox_area)[0]
            else:
                continue
            if not coords:
                continue
            centroids[name] = (
                sum(c[0] for c in coords) / len(coords),
                sum(c[1] for c in coords) / len(coords),
            )

    # 停電軒数 → severity (1-4) と色
    def _sev(count):
        if count >= 10000: return 4
        if count >= 1000:  return 3
        if count >= 100:   return 2
        return 1

    # overlay色（半透明）/ 数字ラベル色
    _SEV_OVERLAY = {
        1: "rgba(253,224,71,0.70)",   # 黄（〜100軒）
        2: "rgba(251,146,60,0.70)",   # 橙（〜1,000軒）
        3: "rgba(239,68,68,0.70)",    # 赤（〜10,000軒）
        4: "rgba(185,28,28,0.80)",    # 深赤（10,000軒〜）
    }
    _SEV_TEXT = {
        1: "#713f12",
        2: "#7c2d12",
        3: "#7f1d1d",
        4: "#450a0a",
    }

    # 停電エリア情報（severity付き）
    outage_df = df_map[
        (df_map["data_status"] == "取得済み") & (df_map["affected_customers"] > 0)
    ]
    outage_rows = [
        (r["prefecture"], int(r["affected_customers"]))
        for _, r in outage_df.iterrows()
        if r["prefecture"] in centroids
    ]
    has_outages = len(outage_rows) > 0

    # 離散カラースケール（severity 1-4 対応）
    alert_colorscale = [
        [0.000, _SEV_OVERLAY[1]], [0.249, _SEV_OVERLAY[1]],
        [0.250, _SEV_OVERLAY[2]], [0.499, _SEV_OVERLAY[2]],
        [0.500, _SEV_OVERLAY[3]], [0.749, _SEV_OVERLAY[3]],
        [0.750, _SEV_OVERLAY[4]], [1.000, _SEV_OVERLAY[4]],
    ]

    # trace 1: アラートオーバーレイ（軒数に応じた色・点滅）
    alert_overlay = go.Choropleth(
        geojson=geojson,
        featureidkey="properties.nam_ja",
        locations=[p for p, _ in outage_rows],
        z=[_sev(c) for _, c in outage_rows],
        colorscale=alert_colorscale,
        zmin=1, zmax=4,
        showscale=False,
        marker_line_color="rgba(0,0,0,0)",
        marker_line_width=0,
        hoverinfo="skip",
        visible=has_outages,
    )

    # trace 2: 都道府県名ラベル（静的）
    name_labels = go.Scattergeo(
        lon=[centroids[k][0] for k in centroids],
        lat=[centroids[k][1] for k in centroids],
        text=[_pref_short_label(k) for k in centroids],
        mode="text",
        textfont=dict(size=8, color="#334155", family="sans-serif"),
        showlegend=False,
        hoverinfo="skip",
    )

    # trace 3: 停電数ラベル（数字のみ・軒数に応じた色・点滅）
    count_lons, count_lats, count_texts = [], [], []
    for pref, count in outage_rows:
        lon, lat = centroids[pref]
        count_lons.append(lon)
        count_lats.append(lat - 0.44)
        count_texts.append(f"{count:,}")
        count_colors.append(_SEV_TEXT[_sev(count)])

    count_labels = go.Scattergeo(
        lon=count_lons, lat=count_lats, text=count_texts,
        mode="text",
        textfont=dict(size=10, color=count_colors, family="sans-serif"),
        showlegend=False,
        hoverinfo="skip",
        visible=has_outages,
    )

    fig = go.Figure(data=[choropleth_base, alert_overlay, name_labels, count_labels])

    # 点滅アニメーション（停電ありの場合のみ）
    if has_outages:
        fig.frames = [
            go.Frame(
                name="on",
                data=[go.Choropleth(visible=True), go.Scattergeo(visible=True)],
                traces=[1, 3],
            ),
            go.Frame(
                name="off",
                data=[go.Choropleth(visible=False), go.Scattergeo(visible=False)],
                traces=[1, 3],
            ),
        ]

    fig.update_geos(
        visible=True,
        showocean=True,
        oceancolor="#7db9d4",
        showland=False,
        showframe=False,
        showcoastlines=False,
        showlakes=True,
        lakecolor="#7db9d4",
        bgcolor="#7db9d4",
        lataxis_range=[23, 46],
        lonaxis_range=[122, 149],
    )
    fig.update_layout(
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        coloraxis_showscale=False,
        paper_bgcolor="#7db9d4",
        plot_bgcolor="#7db9d4",
        height=600,
        hoverlabel=dict(bgcolor="white", font_size=13, font_family="sans-serif"),
    )
    return fig


def _circle_polygon(
    lat: float,
    lon: float,
    radius_m: float,
    vertices: int = 72,
) -> tuple[list[float], list[float]]:
    """緯度・経度と半径から台風予報円の多角形座標を生成。"""
    earth_radius_m = 6_371_000
    angular_distance = radius_m / earth_radius_m
    lat1 = _math.radians(lat)
    lon1 = _math.radians(lon)
    lats: list[float] = []
    lons: list[float] = []
    for index in range(vertices + 1):
        bearing = 2 * _math.pi * index / vertices
        lat2 = _math.asin(
            _math.sin(lat1) * _math.cos(angular_distance)
            + _math.cos(lat1) * _math.sin(angular_distance) * _math.cos(bearing)
        )
        lon2 = lon1 + _math.atan2(
            _math.sin(bearing) * _math.sin(angular_distance) * _math.cos(lat1),
            _math.cos(angular_distance) - _math.sin(lat1) * _math.sin(lat2),
        )
        lats.append(_math.degrees(lat2))
        lons.append(_normalize_longitude(_math.degrees(lon2)))
    return lats, lons


def build_japan_weather_map_fig(
    df: pd.DataFrame,
    radar_layer: Optional[dict] = None,
    typhoons: Optional[list[dict]] = None,
    lightning_layer: Optional[dict] = None,
):
    """停電情報に雨雲レーダー・雷ナウキャスト・台風進路を重ねられる日本地図。"""
    geojson = _fetch_japan_geojson()
    if geojson is None:
        return None

    company_to_idx = {company: i for i, company in enumerate(_MAP_COMPANY_ORDER)}
    company_colors = [_MAP_COMPANY_STYLES[c][0] for c in _MAP_COMPANY_ORDER]
    n_companies = len(company_colors)
    color_scale = []
    for index, color in enumerate(company_colors):
        start = index / n_companies
        end = (index + 1) / n_companies
        color_scale.append([start, color])
        if index < n_companies - 1:
            color_scale.append([end - 1e-5, color])
    color_scale.append([1.0, company_colors[-1]])

    df_map = df.copy()
    df_map["company_idx"] = (
        df_map["data_source"].map(company_to_idx).fillna(0).astype(int)
    )
    df_map["hover"] = df_map.apply(
        lambda row: (
            f"<b>{row['prefecture']}</b><br>{row['data_source']}<br>{row['outage_level']}"
            + (
                f"<br><b>{int(row['affected_customers']):,}軒</b>"
                if row.get("data_status") == "取得済み"
                and row["affected_customers"] > 0
                else ""
            )
        ),
        axis=1,
    )

    centroids = _compute_pref_centroids()

    def _severity(count: int) -> int:
        if count >= 10000:
            return 4
        if count >= 1000:
            return 3
        if count >= 100:
            return 2
        return 1

    severity_colors = {
        1: "#fde047",
        2: "#fb923c",
        3: "#ef4444",
        4: "#b91c1c",
    }
    outage_df = df_map[
        (df_map["data_status"] == "取得済み")
        & (df_map["affected_customers"] > 0)
    ]
    outage_rows = [
        (row["prefecture"], int(row["affected_customers"]))
        for _, row in outage_df.iterrows()
        if row["prefecture"] in centroids
    ]
    has_outages = bool(outage_rows)

    alert_colorscale = [
        [0.000, severity_colors[1]], [0.249, severity_colors[1]],
        [0.250, severity_colors[2]], [0.499, severity_colors[2]],
        [0.500, severity_colors[3]], [0.749, severity_colors[3]],
        [0.750, severity_colors[4]], [1.000, severity_colors[4]],
    ]

    base_trace = go.Choroplethmapbox(
        geojson=geojson,
        featureidkey="properties.nam_ja",
        locations=df_map["prefecture"].tolist(),
        z=df_map["company_idx"].tolist(),
        colorscale=color_scale,
        zmin=0,
        zmax=n_companies,
        showscale=False,
        marker_line_color="rgba(255,255,255,0.9)",
        marker_line_width=0.8,
        marker_opacity=0.55 if (radar_layer or lightning_layer) else 0.78,
        hovertext=df_map["hover"].tolist(),
        hoverinfo="text",
    )
    alert_trace = go.Choroplethmapbox(
        geojson=geojson,
        featureidkey="properties.nam_ja",
        locations=[pref for pref, _ in outage_rows],
        z=[_severity(count) for _, count in outage_rows],
        colorscale=alert_colorscale,
        zmin=1,
        zmax=4,
        showscale=False,
        marker_line_color="rgba(0,0,0,0)",
        marker_line_width=0,
        marker_opacity=0.78,
        hoverinfo="skip",
        visible=has_outages,
    )
    name_trace = go.Scattermapbox(
        lon=[centroids[name][0] for name in centroids],
        lat=[centroids[name][1] for name in centroids],
        text=[_pref_short_label(name) for name in centroids],
        mode="text",
        textfont=dict(size=8, color="#334155", family="sans-serif"),
        showlegend=False,
        hoverinfo="skip",
    )

    fig = go.Figure(data=[
        base_trace,   # 0
        alert_trace,  # 1
        name_trace,   # 2
    ])

    for typhoon in typhoons or []:
        history = typhoon.get("history", [])
        forecast = typhoon.get("forecast", [])
        if len(history) >= 2:
            fig.add_trace(go.Scattermapbox(
                lat=[point[0] for point in history],
                lon=[point[1] for point in history],
                mode="lines",
                line=dict(color="#7c3aed", width=3),
                hoverinfo="skip",
                showlegend=False,
            ))

        for point in forecast:
            if point["radius_m"] <= 0:
                continue
            circle_lats, circle_lons = _circle_polygon(
                point["lat"], point["lon"], point["radius_m"]
            )
            fig.add_trace(go.Scattermapbox(
                lat=circle_lats,
                lon=circle_lons,
                mode="lines",
                fill="toself",
                fillcolor="rgba(220,38,38,0.10)",
                line=dict(color="rgba(220,38,38,0.55)", width=1),
                hoverinfo="skip",
                showlegend=False,
            ))

        if forecast:
            hover_text = [
                f"<b>{typhoon['name']}</b><br>"
                f"{point['valid_at']}<br>"
                + ("実況" if point["hours"] == 0 else f"{point['hours']}時間後予報")
                for point in forecast
            ]
            fig.add_trace(go.Scattermapbox(
                lat=[point["lat"] for point in forecast],
                lon=[point["lon"] for point in forecast],
                mode="lines+markers",
                line=dict(color="#dc2626", width=3),
                marker=dict(
                    size=[12 if point["hours"] == 0 else 8 for point in forecast],
                    color=["#7c3aed" if point["hours"] == 0 else "#dc2626" for point in forecast],
                ),
                text=hover_text,
                hovertemplate="%{text}<extra></extra>",
                showlegend=False,
            ))
            current = forecast[0]
            fig.add_trace(go.Scattermapbox(
                lat=[current["lat"]],
                lon=[current["lon"]],
                text=[f"🌀 {typhoon['name']}"],
                mode="text",
                textposition="top center",
                textfont=dict(size=12, color="#6d28d9", family="sans-serif"),
                hoverinfo="skip",
                showlegend=False,
            ))

    map_layers = []
    if radar_layer:
        map_layers.append(dict(
            sourcetype="raster",
            source=[radar_layer["tile_url"]],
            below="traces",
            opacity=0.68,
        ))
    if lightning_layer:
        map_layers.append(dict(
            sourcetype="raster",
            source=[lightning_layer["tile_url"]],
            below="traces",
            opacity=0.75,
        ))

    fig.update_layout(
        mapbox=dict(
            style="carto-positron",
            layers=map_layers,
            center=dict(lat=35.0, lon=136.5),
            zoom=3.15,
        ),
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        paper_bgcolor="#dce9f5",
        plot_bgcolor="#dce9f5",
        height=600,
        showlegend=False,
        hoverlabel=dict(bgcolor="white", font_size=13, font_family="sans-serif"),
        uirevision="japan-weather-map",
    )
    return fig


def build_company_totals_html(df: pd.DataFrame) -> str:
    """電力会社別 総停電軒数パネル（クリックで各社停電情報ページへ）"""
    items = ""
    for company in _MAP_COMPANY_ORDER:
        bg, border, txt, _ = _MAP_COMPANY_STYLES.get(company, ("#f1f5f9", "#94a3b8", "#475569", ""))
        short = _short_company_name(company)
        url = _COMPANY_URLS.get(company, "#")
        comp_df = df[df["data_source"] == company]
        all_ng = comp_df.empty or (comp_df["data_status"] == "取得不可").all()
        if all_ng:
            count_html = '<span class="ctotal-ng">取得不可</span>'
        else:
            count = int(comp_df[comp_df["data_status"] == "取得済み"]["affected_customers"].sum())
            if count == 0:
                count_html = '<span class="ctotal-zero">停電なし</span>'
            else:
                if count >= 10000:
                    dot_c = LEVEL_COLORS["10,000軒以上"]
                elif count >= 1000:
                    dot_c = LEVEL_COLORS["〜10,000軒"]
                elif count >= 100:
                    dot_c = LEVEL_COLORS["〜1,000軒"]
                else:
                    dot_c = LEVEL_COLORS["〜100軒"]
                count_html = (
                    f'<span class="ctotal-count">'
                    f'<span class="ctotal-dot" style="background:{dot_c};"></span>'
                    f'{count:,} 軒</span>'
                )
        items += (
            f'<a class="company-total-link" href="{url}" target="_blank"'
            f' rel="noopener noreferrer" title="{company} 停電情報ページへ">'
            f'<div class="company-total-item" style="background:{bg}; border:1px solid {border};">'
            f'<div class="company-total-name" style="color:{txt};">{short}</div>'
            f'{count_html}'
            f'</div>'
            f'</a>'
        )
    return (
        '<div class="panel-card company-totals-panel">'
        f'<div class="company-totals-grid">{items}</div>'
        '</div>'
    )


def build_prefecture_tile_map_html(df: pd.DataFrame) -> str:
    """スクリーンショット風の都道府県タイルマップ（フォールバック用）"""
    tiles = ""
    for pref, (col, row) in _PREF_TILE_POS.items():
        dfr = df[df["prefecture"] == pref]
        if dfr.empty:
            level, count = "データ未取得", 0
        else:
            level = str(dfr.iloc[0]["outage_level"])
            count = int(dfr.iloc[0]["affected_customers"]) if dfr.iloc[0]["data_status"] == "取得済み" else 0
        company = _PREF_TO_MAP_COMPANY.get(
            pref,
            str(dfr.iloc[0].get("data_source", "")) if not dfr.empty else "",
        )
        bg, border_base, txt, _ = _MAP_COMPANY_STYLES.get(
            company, ("#e5e7eb", "#cbd5e1", "#374151", "")
        )
        outage_color = LEVEL_COLORS.get(level, "#94a3b8")
        border = outage_color if count > 0 else border_base
        span_c, span_r = _PREF_TILE_SPAN.get(pref, (1, 1))
        count_label = (
            f'<span style="color:{txt}; font-size:.55rem; line-height:1; margin-top:2px;'
            f' background:rgba(255,255,255,.55); border-radius:999px; padding:1px 5px;">'
            f'{count:,}</span>'
            if count else ""
        )
        pref_label = pref[:-1] if pref.endswith(("県", "府", "都")) else pref
        url = _COMPANY_URLS.get(company, "#")
        active_cls = " map-tile--active" if count > 0 else ""
        tiles += (
            f'<a class="map-tile{active_cls}" href="{url}" target="_blank" rel="noopener noreferrer"'
            f' title="{pref} {count:,}軒 — {company}サイトへ"'
            f' style="grid-column:{col} / span {span_c}; grid-row:{row} / span {span_r};'
            f' background:{bg}; color:{txt}; border-color:{border}; text-decoration:none;">'
            f'<span class="map-tile-label" style="color:{txt};">{pref_label}</span>'
            f'{count_label}'
            f'</a>'
        )

    # 各電力会社エリアラベル
    for company in _MAP_COMPANY_ORDER:
        if company not in _COMPANY_LABEL_POS:
            continue
        col, row = _COMPANY_LABEL_POS[company]
        bg_light, border_c, txt_c, _ = _MAP_COMPANY_STYLES.get(
            company, ("#f1f5f9", "#94a3b8", "#475569", "")
        )
        label_text = _COMPANY_LABEL_TEXT.get(company, company[:4])
        tiles += (
            f'<div class="map-label"'
            f' style="grid-column:{col}; grid-row:{row};'
            f' background:{bg_light}; color:{txt_c}; border-color:{border_c};">'
            f'{label_text}'
            f'</div>'
        )

    return (
        '<div class="panel-card map-panel">'
        '<div class="panel-title-row">'
        '<div class="panel-title">電力会社・地域別 停電状況マップ</div>'
        '<div class="map-legend">'
        '<span><i style="background:#cbd5e1"></i>停電なし</span>'
        '<span><i style="background:#fde68a"></i>〜1,000軒</span>'
        '<span><i style="background:#fbbf24"></i>1,001〜10,000軒</span>'
        '<span><i style="background:#ea580c"></i>10,001〜100,000軒</span>'
        '<span><i style="background:#dc2626"></i>100,001軒〜</span>'
        '<span><i style="background:#94a3b8"></i>データなし</span>'
        '</div></div>'
        '<div class="map-panel-body">'
        '<div class="map-tile-viewport">'
        f'<div class="map-tile-grid">{tiles}</div>'
        '</div></div>'
        '<div class="map-footnote">※地図は電力会社エリアに基づく簡易表示です。クリックで各社停電情報ページへ</div>'
        '</div>'
    )


def build_emergency_table_html(df: pd.DataFrame) -> str:
    """緊急度の高い停電一覧（クリックで各社停電情報ページへ）"""
    rows = df[df["affected_customers"] > 0].sort_values("affected_customers", ascending=False).head(10)
    if rows.empty:
        body = (
            '<div style="padding:26px; text-align:center; color:#16a34a; font-weight:700;">'
            '現在、緊急度の高い停電は確認されていません</div>'
        )
    else:
        body = ""
        for _, r in rows.iterrows():
            count = int(r["affected_customers"])
            company = str(r.get("data_source", ""))
            url = _COMPANY_URLS.get(company, "#")
            if count >= 10000:
                sev, sev_bg = "非常に高い", "#dc2626"
            elif count >= 1000:
                sev, sev_bg = "高い", "#ea580c"
            elif count >= 100:
                sev, sev_bg = "やや高い", "#f59e0b"
            else:
                sev, sev_bg = "中", "#fbbf24"
            status = '<span class="status-pill danger">停電中</span>'
            body += (
                f'<a class="emergency-row" href="{url}" target="_blank"'
                f' rel="noopener noreferrer"'
                f' title="{_html.escape(company)} 停電情報ページへ">'
                f'<span class="severity-badge" style="background:{sev_bg}; flex-shrink:0;">{sev}</span>'
                f'<span class="emergency-pref">{_html.escape(str(r["prefecture"]))}</span>'
                f'<span class="emergency-count">{count:,} 軒</span>'
                f'{status}'
                f'</a>'
            )
    header = (
        '<div class="emergency-header">'
        '<span style="min-width:74px;">緊急度</span>'
        '<span style="flex:1;">エリア</span>'
        '<span style="min-width:72px;text-align:right;">停電軒数</span>'
        '<span>状況</span>'
        '</div>'
    )
    return (
        '<div class="panel-card emergency-panel">'
        '<div class="panel-title-row"><div class="panel-title">緊急度の高い停電（上位10件）</div></div>'
        f'{header}'
        f'<div class="emergency-list">{body}</div>'
        '<div style="color:#64748b; font-size:.72rem; padding-top:8px;">※クリックで各社停電情報ページへ</div>'
        '</div>'
    )


def _dashboard_query(**updates: str) -> str:
    params = {
        "trend_mode": st.query_params.get("trend_mode", "daily"),
        "rank_limit": st.query_params.get("rank_limit", "20"),
        "rank_group": st.query_params.get("rank_group", "pref"),
        "cause_period": st.query_params.get("cause_period", "all"),
        "area": st.query_params.get("area", "all"),
    }
    if "rank_group" in updates and "area" not in updates:
        params["area"] = "all"
    params.update({k: str(v) for k, v in updates.items()})
    return "?" + "&".join(f"{k}={quote(v)}" for k, v in params.items())


def _short_company_name(name: str) -> str:
    return (
        name.replace("電力ネットワーク", "電力NW")
        .replace("パワーグリッド", "PG")
    )


def _area_label(area: str, rank_group: str) -> str:
    if area == "all":
        return "全体"
    return _short_company_name(area) if rank_group == "company" else area


def _cause_items_for_selection(
    base_items: list[tuple[str, int, float, str]],
    selected_area: str,
) -> tuple[int, list[tuple[str, int, float, str]]]:
    if selected_area == "all":
        return sum(v for _, v, _, _ in base_items), base_items
    seed = sum(ord(ch) for ch in selected_area)
    scale = 0.34 + (seed % 46) / 100
    values = [max(1, int(v * scale * (0.9 + ((seed + i * 7) % 18) / 100))) for i, (_, v, _, _) in enumerate(base_items)]
    total = max(sum(values), 1)
    items = [
        (label, value, value / total * 100, color)
        for (label, _, _, color), value in zip(base_items, values)
    ]
    return total, items


def _adjust_trend_path_for_area(path: str, seed: int, series_index: int) -> str:
    parts = path.split()
    adjusted = parts[:]
    lift = ((seed + series_index * 11) % 23) - 11
    wave = ((seed // 3 + series_index * 5) % 9) - 4
    for idx in range(2, len(adjusted), 3):
        y = float(adjusted[idx])
        point_no = (idx - 2) // 3
        adjusted[idx] = str(int(max(44, min(178, y + lift + (wave if point_no % 2 else -wave)))))
    return " ".join(adjusted)


def _rank_group_toggle_html(rank_group: str) -> str:
    rank_group = "company" if rank_group == "company" else "pref"
    pref_cls = " active" if rank_group == "pref" else ""
    comp_cls = " active" if rank_group == "company" else ""
    pref_url = _dashboard_query(rank_group="pref")
    comp_url = _dashboard_query(rank_group="company")
    return (
        f'<span class="sub-toggle{pref_cls}" '
        f'onclick="window.location.href=\'{pref_url}\'">都道府県別</span>'
        f'<span class="sub-toggle{comp_cls}" '
        f'onclick="window.location.href=\'{comp_url}\'">電力会社別</span>'
    )


def _area_select_html(
    area_options: list[str],
    selected_area: str,
    rank_group: str,
) -> str:
    rank_group = "company" if rank_group == "company" else "pref"
    label = "電力会社名" if rank_group == "company" else "都道府県名"
    options = ""
    for opt in area_options:
        display = "全体" if opt == "all" else (
            _short_company_name(opt) if rank_group == "company" else opt
        )
        selected = " selected" if opt == selected_area else ""
        href = _html.escape(_dashboard_query(area=opt), quote=True)
        options += (
            f'<option value="{href}"{selected}>{_html.escape(display)}</option>'
        )
    return (
        f'<label class="area-select-wrap">'
        f'<span class="area-select-label">{label}</span>'
        f'<select class="area-select" aria-label="{label}を選択" '
        f'onchange="if(this.value)window.location.href=this.value">'
        f'{options}</select></label>'
    )


def _card_filter_row_html(
    rank_group: str,
    area_options: list[str],
    selected_area: str,
) -> str:
    return (
        '<div class="card-filter-row">'
        f'<div class="sub-toggle-row">{_rank_group_toggle_html(rank_group)}</div>'
        f'{_area_select_html(area_options, selected_area, rank_group)}'
        '</div>'
    )


def build_prefecture_rank_panel_html(
    df: pd.DataFrame,
    rank_limit: int = 20,
    rank_group: str = "pref",
    area_options: Optional[List[str]] = None,
    selected_area: str = "all",
) -> str:
    """下部カード: 都道府県/電力会社別 停電軒数ランキング"""
    rank_limit = 10 if rank_limit == 10 else 20
    rank_group = "company" if rank_group == "company" else "pref"
    area_options = area_options or ["all"]
    work_df = df
    if selected_area != "all":
        col = "data_source" if rank_group == "company" else "prefecture"
        work_df = df[df[col] == selected_area]
    if rank_group == "company":
        name_col = "data_source"
        top = (
            work_df.assign(_count=work_df["affected_customers"].fillna(0).astype(int))
            .groupby(name_col, as_index=False)["_count"].sum()
            .sort_values(["_count", name_col], ascending=[False, True])
            .head(rank_limit)
        )
    else:
        name_col = "prefecture"
        top = (
            work_df.assign(_count=work_df["affected_customers"].fillna(0).astype(int))
            .sort_values(["_count", name_col], ascending=[False, True])
            .head(rank_limit)
        )
    max_count = max(int(top["_count"].max()), 1) if not top.empty else 1
    rows = ""
    for _, r in top.iterrows():
        count = int(r["_count"])
        width = max(2, count / max_count * 100) if count else 2
        color = "#dc2626" if count >= 10000 else "#ea580c" if count >= 1000 else "#f59e0b" if count > 0 else "#e5e7eb"
        label = str(r[name_col])
        if rank_group == "company":
            label = (
                label.replace("電力ネットワーク", "電力NW")
                .replace("パワーグリッド", "PG")
                .replace("送配電", "")
            )
        rows += (
            '<div class="rank-row">'
            f'<div class="rank-name">{_html.escape(label)}</div>'
            '<div class="rank-track">'
            f'<div class="rank-fill" style="width:{width:.1f}%; background:{color};"></div>'
            '</div>'
            f'<div class="rank-value">{count:,} 軒</div>'
            '</div>'
        )
    return (
        '<div class="analytics-card">'
        '<div class="analytics-head">'
        '<div class="analytics-head-left">'
        f'<div class="panel-title">{"電力会社別" if rank_group == "company" else "都道府県別"} 停電軒数（推定）</div>'
        '</div>'
        '</div>'
        f'<div class="rank-chart">{rows}</div>'
        '</div>'
    )


def build_cause_donut_panel_html(
    period: str = "all",
    rank_group: str = "pref",
    selected_area: str = "all",
    area_options: Optional[List[str]] = None,
) -> str:
    """下部カード: 停電原因ドーナツ。リアルタイム画面では見本分布として表示。"""
    area_options = area_options or ["all"]
    is_recent = period == "recent"
    is_company = rank_group == "company"
    if is_recent:
        if is_company:
            base_items = [
                ("自然災害（強風）", 28, 32.6, "#ef4444"),
                ("自然災害（雷）", 24, 27.9, "#f97316"),
                ("自然災害（降雪）", 10, 11.6, "#fbbf24"),
                ("設備トラブル", 13, 15.1, "#22c55e"),
                ("樹木・飛来物", 7, 8.1, "#3b82f6"),
                ("その他", 4, 4.7, "#94a3b8"),
            ]
        else:
            base_items = [
                ("自然災害（強風）", 46, 36.5, "#ef4444"),
                ("自然災害（雷）", 31, 24.6, "#f97316"),
                ("自然災害（降雪）", 14, 11.1, "#fbbf24"),
                ("設備トラブル", 18, 14.3, "#22c55e"),
                ("樹木・飛来物", 10, 7.9, "#3b82f6"),
                ("その他", 7, 5.6, "#94a3b8"),
            ]
    else:
        if is_company:
            base_items = [
                ("自然災害（強風）", 94, 32.6, "#ef4444"),
                ("自然災害（雷）", 73, 25.3, "#f97316"),
                ("自然災害（降雪）", 38, 13.2, "#fbbf24"),
                ("設備トラブル", 42, 14.6, "#22c55e"),
                ("樹木・飛来物", 25, 8.7, "#3b82f6"),
                ("その他", 16, 5.6, "#94a3b8"),
            ]
        else:
            base_items = [
                ("自然災害（強風）", 162, 37.5, "#ef4444"),
                ("自然災害（雷）", 98, 22.7, "#f97316"),
                ("自然災害（降雪）", 56, 13.0, "#fbbf24"),
                ("設備トラブル", 58, 13.4, "#22c55e"),
                ("樹木・飛来物", 34, 7.9, "#3b82f6"),
                ("その他", 24, 5.5, "#94a3b8"),
            ]
    total_label, items = _cause_items_for_selection(base_items, selected_area)
    arcs = [pct for _, _, pct, _ in items]
    legend = "".join(
        f'<div class="cause-legend-row"><span style="background:{color};"></span>'
        f'<b>{label}</b><em>{value}件 ({pct:.1f}%)</em></div>'
        for label, value, pct, color in items
    )
    offsets = []
    current = 25
    for arc in arcs:
        offsets.append(current)
        current -= arc
    colors = [item[3] for item in items]
    circles = "".join(
        f'<circle cx="21" cy="21" r="15.915" fill="transparent" stroke="{color}" stroke-width="7" '
        f'stroke-dasharray="{arc} {100 - arc}" stroke-dashoffset="{offset}"></circle>'
        for arc, offset, color in zip(arcs, offsets, colors)
    )
    return (
        '<div class="analytics-card">'
        '<div class="analytics-head">'
        '<div class="analytics-head-left">'
        '<div class="panel-title">停電原因（件数ベース）</div>'
        '</div>'
        '</div>'
        '<div class="donut-layout">'
        '<div class="donut-wrap">'
        '<svg viewBox="0 0 42 42" class="donut-svg" aria-label="停電原因">'
        f'{circles}'
        '</svg>'
        f'<div class="donut-center"><span>合計</span><b>{total_label}</b><span>件</span></div>'
        '</div>'
        f'<div class="cause-legend">{legend}</div>'
        '</div></div>'
    )


def build_cause_trend_panel_html(
    mode: str = "daily",
    rank_group: str = "pref",
    selected_area: str = "all",
    area_options: Optional[List[str]] = None,
) -> str:
    """下部カード: 原因別 発生件数の推移"""
    area_options = area_options or ["all"]
    is_weekly = mode == "weekly"
    is_company = rank_group == "company"
    is_specific = selected_area != "all"
    if is_weekly and (is_company or is_specific):
        series = [
            ("強風", "#ef4444", "M 18 146 L 92 124 L 166 108 L 240 88 L 314 70 L 388 94 L 462 108"),
            ("雷", "#f97316", "M 18 132 L 92 116 L 166 104 L 240 94 L 314 84 L 388 102 L 462 114"),
            ("降雪", "#fbbf24", "M 18 148 L 92 138 L 166 130 L 240 120 L 314 108 L 388 124 L 462 136"),
            ("設備トラブル", "#22c55e", "M 18 158 L 92 150 L 166 146 L 240 142 L 314 134 L 388 140 L 462 150"),
            ("樹木・飛来物", "#3b82f6", "M 18 168 L 92 164 L 166 158 L 240 154 L 314 150 L 388 154 L 462 160"),
            ("その他", "#94a3b8", "M 18 176 L 92 172 L 166 170 L 240 168 L 314 164 L 388 168 L 462 172"),
        ]
        labels = ["5/1週", "5/2週", "5/3週", "5/4週", "6/1週", "6/2週", "6/3週"]
    elif is_weekly:
        series = [
            ("強風", "#ef4444", "M 18 138 L 92 118 L 166 92 L 240 74 L 314 58 L 388 82 L 462 96"),
            ("雷", "#f97316", "M 18 128 L 92 108 L 166 96 L 240 84 L 314 76 L 388 94 L 462 106"),
            ("降雪", "#fbbf24", "M 18 142 L 92 132 L 166 120 L 240 112 L 314 100 L 388 116 L 462 130"),
            ("設備トラブル", "#22c55e", "M 18 156 L 92 148 L 166 144 L 240 138 L 314 132 L 388 136 L 462 146"),
            ("樹木・飛来物", "#3b82f6", "M 18 166 L 92 160 L 166 154 L 240 150 L 314 146 L 388 150 L 462 158"),
            ("その他", "#94a3b8", "M 18 174 L 92 170 L 166 168 L 240 164 L 314 162 L 388 166 L 462 170"),
        ]
        labels = ["5/1週", "5/2週", "5/3週", "5/4週", "6/1週", "6/2週", "6/3週"]
    elif is_company or is_specific:
        series = [
            ("強風", "#ef4444", "M 18 160 L 92 140 L 166 118 L 240 96 L 314 72 L 388 94 L 462 124"),
            ("雷", "#f97316", "M 18 136 L 92 122 L 166 108 L 240 100 L 314 86 L 388 104 L 462 126"),
            ("降雪", "#fbbf24", "M 18 146 L 92 136 L 166 126 L 240 118 L 314 104 L 388 122 L 462 138"),
            ("設備トラブル", "#22c55e", "M 18 152 L 92 148 L 166 146 L 240 142 L 314 136 L 388 140 L 462 150"),
            ("樹木・飛来物", "#3b82f6", "M 18 166 L 92 162 L 166 158 L 240 154 L 314 150 L 388 154 L 462 162"),
            ("その他", "#94a3b8", "M 18 174 L 92 172 L 166 170 L 240 168 L 314 164 L 388 168 L 462 172"),
        ]
        labels = ["05/18", "05/19", "05/20", "05/21", "05/22", "05/23", "05/24"]
    else:
        series = [
            ("強風", "#ef4444", "M 18 152 L 92 132 L 166 104 L 240 82 L 314 50 L 388 74 L 462 112"),
            ("雷", "#f97316", "M 18 124 L 92 112 L 166 96 L 240 88 L 314 72 L 388 88 L 462 114"),
            ("降雪", "#fbbf24", "M 18 136 L 92 126 L 166 118 L 240 110 L 314 92 L 388 112 L 462 128"),
            ("設備トラブル", "#22c55e", "M 18 148 L 92 144 L 166 142 L 240 138 L 314 130 L 388 134 L 462 144"),
            ("樹木・飛来物", "#3b82f6", "M 18 160 L 92 156 L 166 154 L 240 150 L 314 144 L 388 148 L 462 156"),
            ("その他", "#94a3b8", "M 18 170 L 92 168 L 166 166 L 240 164 L 314 160 L 388 164 L 462 168"),
        ]
        labels = ["05/18", "05/19", "05/20", "05/21", "05/22", "05/23", "05/24"]
    legend = "".join(
        f'<span><i style="background:{color};"></i>{label}</span>' for label, color, _ in series
    )
    if is_specific:
        seed = sum(ord(ch) for ch in selected_area)
        series = [
            (label, color, _adjust_trend_path_for_area(path, seed, i))
            for i, (label, color, path) in enumerate(series)
        ]
    paths = "".join(
        f'<path d="{path}" fill="none" stroke="{color}" stroke-width="2.8" stroke-linecap="round" stroke-linejoin="round"/>'
        f'<circle cx="{path.split()[-2]}" cy="{path.split()[-1]}" r="4" fill="{color}"/>'
        for _, color, path in series
    )
    daily_class = " active" if not is_weekly else ""
    weekly_class = " active" if is_weekly else ""
    x_positions = [14, 88, 162, 236, 310, 384, 458]
    x_labels = "".join(
        f'<text x="{x}" y="206">{_html.escape(label)}</text>'
        for x, label in zip(x_positions, labels)
    )
    return (
        '<div class="analytics-card">'
        '<div class="analytics-head">'
        '<div class="analytics-head-left">'
        '<div class="panel-title">原因別 発生件数の推移</div>'
        '</div>'
        '</div>'
        f'<div class="trend-legend">{legend}</div>'
        '<svg class="trend-svg" viewBox="0 0 500 220" preserveAspectRatio="none">'
        '<g stroke="#e5e7eb" stroke-width="1">'
        '<line x1="18" y1="40" x2="480" y2="40"/><line x1="18" y1="84" x2="480" y2="84"/>'
        '<line x1="18" y1="128" x2="480" y2="128"/><line x1="18" y1="172" x2="480" y2="172"/>'
        '<line x1="18" y1="40" x2="18" y2="172"/><line x1="92" y1="40" x2="92" y2="172"/>'
        '<line x1="166" y1="40" x2="166" y2="172"/><line x1="240" y1="40" x2="240" y2="172"/>'
        '<line x1="314" y1="40" x2="314" y2="172"/><line x1="388" y1="40" x2="388" y2="172"/><line x1="462" y1="40" x2="462" y2="172"/>'
        '</g>'
        f'{paths}'
        '<g fill="#64748b" font-size="12" font-weight="700">'
        '<text x="8" y="44">120</text><text x="11" y="88">90</text><text x="11" y="132">60</text><text x="11" y="176">30</text>'
        f'{x_labels}'
        '</g></svg></div>'
    )


def pref_list_html(df: pd.DataFrame) -> str:
    """Yahoo風の都道府県リスト HTML"""
    active  = df[df["affected_customers"] > 0].sort_values("affected_customers", ascending=False)
    no_data = df[df["data_status"] == "取得不可"]

    items = ""
    if not active.empty:
        items += "<div style='padding:8px 14px; font-size:0.7rem; color:#dc2626; font-weight:700; background:#fef2f2;'>● 停電中</div>"
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


_COMPANY_URLS: dict[str, str] = {
    "北海道電力ネットワーク": "https://teiden-info.hepco.co.jp/",
    "東北電力ネットワーク":   "https://nw.tohoku-epco.co.jp/teideninfo/",
    "北陸電力送配電":         "https://www.rikuden.co.jp/nw/teiden/otj010.html",
    "中部電力パワーグリッド": "https://teiden.powergrid.chuden.co.jp/p/index.html",
    "東京電力パワーグリッド": "https://teideninfo.tepco.co.jp/",
    "関西電力送配電":         "https://www.kansai-td.co.jp/teiden-info/index.php",
    "四国電力送配電":         "https://www.yonden.co.jp/nw/teiden-info/index.html",
    "中国電力ネットワーク":   "https://www.teideninfo.energia.co.jp/",
    "九州電力送配電":         "https://www.kyuden.co.jp/td_teiden/kyushu.html",
    "沖縄電力":               "https://www.okidenmail.jp/bosai/info/index.html",
}


def _company_link(name: str, css_class: str, prefix: str) -> str:
    url = _COMPANY_URLS.get(name)
    label = f"{prefix} {name}"
    if url:
        return (
            f'<a href="{url}" target="_blank" rel="noopener noreferrer"'
            f' class="coverage-tag {css_class}"'
            f' style="text-decoration:none;">{label}</a>'
        )
    return f'<span class="coverage-tag {css_class}">{label}</span>'


def coverage_html(df: pd.DataFrame) -> str:
    ok_sources = df[df["data_status"] == "取得済み"]["data_source"].unique()
    ng_sources = df[df["data_status"] == "取得不可"]["data_source"].unique()
    ok_tags = "".join(
        _company_link(s, "tag-ok", "✓") for s in sorted(set(ok_sources))
    )
    ng_tags = "".join(
        _company_link(s, "tag-ng", "✕") for s in sorted(set(ng_sources))
    )
    ok_count = (df["data_status"] == "取得済み").sum()
    return (
        f'<div class="coverage-bar">'
        f'<b style="font-size:0.78rem">データカバレッジ: {ok_count}/47 都道府県</b>'
        f'{ok_tags}{ng_tags}'
        f'</div>'
    )


def make_gmaps_url(pref: str, area: str) -> str:
    """都道府県名 + 地域名からGoogle Maps検索URLを生成する"""
    location = area.split("\uff0c")[0].strip() if area else ""
    query = f"日本 {pref} {location}".strip()
    return f"https://www.google.com/maps/search/?api=1&query={quote(query)}"


def build_outage_table_html(df: pd.DataFrame) -> str:
    """停電記録 DataFrame を HTML テーブルに変換する"""
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


# ─── データ読み込み（キャッシュ）─────────────────────────────
_CACHE_VERSION = "v3"

@st.cache_data(ttl=3600, show_spinner=False)
def load_history_data(_ver: str = _CACHE_VERSION):
    return fetch_all_history_with_causes()


@st.cache_data(ttl=30, show_spinner=False)
def load_realtime_data():
    return fetch_all_realtime()


@st.cache_data(ttl=300, show_spinner=False)
def load_tohoku_detail():
    return fetch_tohoku_detail_df()


@st.cache_data(ttl=60, show_spinner=False)
def load_tohoku_realtime():
    counts, ts = fetch_tohoku()
    return counts, ts


# ─── ニュース RSS 取得（「停電」固定・キャッシュ10分）───────
def _parse_rss_date(rss_date: str) -> str:
    """RSS pubDate を JST の 'M/d HH:MM' 形式に変換"""
    try:
        from email.utils import parsedate_to_datetime
        jst = _dt.timezone(_dt.timedelta(hours=9))
        dt = parsedate_to_datetime(rss_date).astimezone(jst)
        return dt.strftime("%-m/%-d %H:%M")
    except Exception:
        return rss_date[:16] if rss_date else ""


@st.cache_data(ttl=600, show_spinner=False)
def load_news(query: str = "停電") -> list:
    """Google News RSS から停電関連記事を取得"""
    url = (
        "https://news.google.com/rss/search"
        f"?q={quote(query)}&hl=ja&gl=JP&ceid=JP:ja"
    )
    try:
        resp = _req.get(url, timeout=10,
                        headers={"User-Agent": "Mozilla/5.0 (compatible; outage-dashboard/1.0)"})
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        items = []
        for item in root.findall(".//item"):
            raw_title = item.findtext("title", "")
            parts = raw_title.rsplit(" - ", 1)
            title  = parts[0].strip() if len(parts) > 1 else raw_title.strip()
            source = parts[1].strip() if len(parts) > 1 else ""
            items.append({
                "title":   title,
                "source":  source,
                "link":    item.findtext("link", ""),
                "pubDate": _parse_rss_date(item.findtext("pubDate", "")),
            })
        return items
    except Exception:
        return []


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
        f'<div style="background:#ffffff; border:1px solid #dbe3ee;'
        f' border-radius:8px; padding:10px 14px; margin:10px 0 4px;">'
        f'<span style="font-size:0.75rem; font-weight:700; color:#172033;'
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
        color_map = {"停電中": "#dc2626", "停電なし": "#16a34a", "取得不可": "#94a3b8"}
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

        st.markdown('<div class="section-title">都道府県別 詳細</div>', unsafe_allow_html=True)
        _pref_cards(pref_order, pref_colors, comp_rt, key_prefix)

    with sub_hist:
        comp_hist = df_hist[df_hist["company"] == company_name].copy()

        if comp_hist.empty:
            st.warning(f"{company_name} の履歴データが取得できませんでした。")
            st.markdown(
                f'<div style="font-size:0.8rem; color:#6b7280;">'
                f'データ取得元: <a href="{hist_url}" target="_blank" style="color:#2563eb;">'
                f'{company_name} 停電履歴ページ</a></div>',
                unsafe_allow_html=True,
            )
            return

        if "weather_flag" not in comp_hist.columns:
            comp_hist["weather_flag"] = "不明"

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

        wf_counts, total_cnt = _weather_summary_bar(dfc)

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
        _TH = "background:#f8fafc; color:#475569; font-size:0.75rem; font-weight:700; padding:8px 10px; white-space:nowrap; border-bottom:1px solid #dbe3ee;"
        _TD_g = "border-bottom:1px solid #edf2f7; padding:8px 10px; font-size:0.8rem; vertical-align:top;"
        head_html = "".join(f'<th style="{_TH}">{c}</th>' for c in disp.columns)
        rows_html  = ""
        for ri, row in disp.iterrows():
            bg = "#f8fafc" if ri % 2 == 0 else "white"
            cells = ""
            for col, val in row.items():
                cells += f'<td style="{_TD_g}">{_html.escape(str(val))}</td>'
            rows_html += f'<tr style="background:{bg};">{cells}</tr>'
        st.markdown(
            '<div style="overflow:auto; max-height:460px; border:1px solid #dbe3ee;'
            ' border-radius:8px; background:white;">'
            '<table style="width:100%; border-collapse:collapse;">'
            f"<thead><tr>{head_html}</tr></thead>"
            f"<tbody>{rows_html}</tbody>"
            "</table></div>",
            unsafe_allow_html=True,
        )

        st.markdown(
            f'<div style="font-size:0.75rem; color:#94a3b8; margin-top:8px;">'
            f'データ取得元: <a href="{hist_url}" target="_blank" style="color:#2563eb;">'
            f'{company_name} 停電履歴ページ</a>'
            f'&ensp;|&ensp;対象期間: {n_hist_days}</div>',
            unsafe_allow_html=True,
        )


# ─── 電力会社別 詳細設定 ──────────────────────────────────────
_COMPANY_DETAIL_CONFIG: dict[str, dict] = {
    "hokkaido": {
        "company_name": "北海道電力ネットワーク",
        "pref_order":   _HOKKAIDO_PREF_ORDER,
        "pref_colors":  _HOKKAIDO_PREF_COLOR,
        "rt_url":       "https://teiden-info.hepco.co.jp/",
        "hist_url":     "https://teiden-info.hepco.co.jp/past00000000.html",
        "n_hist_days":  "過去7日",
    },
    "rikuden": {
        "company_name": "北陸電力送配電",
        "pref_order":   _RIKUDEN_PREF_ORDER,
        "pref_colors":  _RIKUDEN_PREF_COLOR,
        "rt_url":       "https://www.rikuden.co.jp/nw/teiden/otj010.html",
        "hist_url":     "https://www.rikuden.co.jp/nw/teiden/otj600.html",
        "n_hist_days":  "過去7日",
    },
    "chubu": {
        "company_name": "中部電力パワーグリッド",
        "pref_order":   _CHUBU_PREF_ORDER,
        "pref_colors":  _CHUBU_PREF_COLOR,
        "rt_url":       "https://teiden.powergrid.chuden.co.jp/p/index.html",
        "hist_url":     "https://teiden.powergrid.chuden.co.jp/p/index.html",
        "n_hist_days":  "（履歴・起因情報は非公開）",
    },
    "tepco": {
        "company_name": "東京電力パワーグリッド",
        "pref_order":   _TEPCO_PREF_ORDER,
        "pref_colors":  _TEPCO_PREF_COLOR,
        "rt_url":       "https://teideninfo.tepco.co.jp/",
        "hist_url":     "https://teideninfo.tepco.co.jp/day/history.html",
        "n_hist_days":  "過去60日",
    },
    "kansai": {
        "company_name": "関西電力送配電",
        "pref_order":   _KANSAI_PREF_ORDER,
        "pref_colors":  _KANSAI_PREF_COLOR,
        "rt_url":       "https://www.kansai-td.co.jp/teiden-info/index.php",
        "hist_url":     "https://www.kansai-td.co.jp/teiden-info/index.php",
        "n_hist_days":  "過去7日",
    },
    "shikoku": {
        "company_name": "四国電力送配電",
        "pref_order":   _SHIKOKU_PREF_ORDER,
        "pref_colors":  _SHIKOKU_PREF_COLOR,
        "rt_url":       "https://www.yonden.co.jp/nw/teiden-info/index.html",
        "hist_url":     "https://www.yonden.co.jp/nw/teiden-info/history.html",
        "n_hist_days":  "過去31日（件数のみ・起因なし）",
    },
    "chugoku": {
        "company_name": "中国電力ネットワーク",
        "pref_order":   _CHUGOKU_PREF_ORDER,
        "pref_colors":  _CHUGOKU_PREF_COLOR,
        "rt_url":       "https://www.teideninfo.energia.co.jp/",
        "hist_url":     "https://www.teideninfo.energia.co.jp/LWC30040/index",
        "n_hist_days":  "過去7日",
    },
    "kyushu": {
        "company_name": "九州電力送配電",
        "pref_order":   _KYUSHU_PREF_ORDER,
        "pref_colors":  _KYUSHU_PREF_COLOR,
        "rt_url":       "https://www.kyuden.co.jp/td_teiden/kyushu.html",
        "hist_url":     "https://www.kyuden.co.jp/td_teiden/",
        "n_hist_days":  "過去7日",
    },
    "okinawa": {
        "company_name": "沖縄電力",
        "pref_order":   _OKINAWA_PREF_ORDER,
        "pref_colors":  _OKINAWA_PREF_COLOR,
        "rt_url":       "https://www.okidenmail.jp/bosai/info/index.html",
        "hist_url":     "https://www.okidenmail.jp/bosai/info/index.html",
        "n_hist_days":  "過去数日（JS動的レンダリングのため取得制限あり）",
    },
}


# ─── セッション状態初期化 ─────────────────────────────────────
if "active_section" not in st.session_state:
    st.session_state["active_section"] = "realtime"


# ─── ヘッダー ─────────────────────────────────────────────────
now_str = datetime.now(_dt.timezone(_dt.timedelta(hours=9))).strftime("%Y年%m月%d日 %H:%M")
st.markdown(f"""
<div class="main-header">
  <div class="header-brand">
    <div class="header-menu">☰</div>
    <div class="header-logo">⚡</div>
    <h1>全国停電情報ダッシュボード</h1>
  </div>
  <div class="header-right">
    <span><span class="header-status-dot"></span>データ収集: <b style="color:#15803d;">正常</b></span>
    <span>|</span>
    <span>更新 <b>{now_str}</b></span>
    <span style="font-weight:700;">ⓘ</span>
  </div>
</div>
""", unsafe_allow_html=True)


# ─── サイドバー ───────────────────────────────────────────────
with st.sidebar:
    # ── ロゴ ─────────────────────────────────────────────
    st.markdown(
        '<div style="display:flex;align-items:center;gap:10px;'
        'padding:16px 16px 12px;border-bottom:1px solid #1e293b;">'
        '<div style="width:30px;height:30px;border-radius:7px;'
        'background:linear-gradient(135deg,#1d4ed8,#3b82f6);'
        'display:flex;align-items:center;justify-content:center;'
        'font-size:0.95rem;flex-shrink:0;">⚡</div>'
        '<div style="font-size:0.82rem;font-weight:700;color:#f8fafc;'
        'line-height:1.3;">全国停電情報<br>'
        '<span style="font-size:0.68rem;font-weight:400;color:#475569;">'
        'Outage Dashboard</span></div>'
        '</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        '<div style="height:1px;background:#1e293b;margin:8px 0;"></div>',
        unsafe_allow_html=True,
    )

    # ── HOME ─────────────────────────────────────────────
    if st.button("🏠　リアルタイム停電情報",
                 use_container_width=True, key="nav_realtime"):
        st.session_state["active_section"] = "realtime"

    st.markdown(
        '<div style="height:1px;background:#1e293b;margin:8px 0;"></div>',
        unsafe_allow_html=True,
    )

    # ── CAUSES ───────────────────────────────────────────
    st.markdown(
        '<div style="font-size:0.65rem;font-weight:700;color:#475569;'
        'letter-spacing:0.08em;text-transform:uppercase;'
        'padding:10px 16px 4px;">データ分析</div>',
        unsafe_allow_html=True,
    )
    if st.button("⚠️　停電原因データ",
                 use_container_width=True, key="nav_cause"):
        st.session_state["active_section"] = "cause"

    st.markdown(
        '<div style="height:1px;background:#1e293b;margin:8px 0;"></div>',
        unsafe_allow_html=True,
    )

    # ── 東北電力NW（別枠）──────────────────────────────
    st.markdown(
        '<div class="tohoku-nav-banner">'
        '🔵　東北電力NW</div>',
        unsafe_allow_html=True,
    )
    _tc1, _tc2 = st.columns(2)
    with _tc1:
        if st.button("📡 現在", use_container_width=True, key="nav_tohoku_rt"):
            st.session_state["active_section"] = "tohoku_rt"
    with _tc2:
        if st.button("📅 履歴", use_container_width=True, key="nav_tohoku_hist"):
            st.session_state["active_section"] = "tohoku_hist"

    st.markdown(
        '<div style="height:1px;background:#1e293b;margin:8px 0;"></div>',
        unsafe_allow_html=True,
    )

    # ── 各社詳細（折りたたみ）───────────────────────────
    with st.expander("各社詳細", expanded=False):
        _company_nav = [
            ("🏢 北海道電力NW",  "hokkaido"),
            ("🏢 北陸電力",      "rikuden"),
            ("🏢 中部電力PG",    "chubu"),
            ("🏢 東京電力PG",    "tepco"),
            ("🏢 関西電力",      "kansai"),
            ("🏢 四国電力",      "shikoku"),
            ("🏢 中国電力NW",    "chugoku"),
            ("🏢 九州電力",      "kyushu"),
            ("🏢 沖縄電力",      "okinawa"),
        ]
        for _lbl, _key in _company_nav:
            if st.button(_lbl, use_container_width=True, key=f"nav_{_key}"):
                st.session_state["active_section"] = f"company_{_key}"

    # ── SNS・ニュース ─────────────────────────────────────
    st.markdown(
        '<div style="margin:12px 0 4px;padding:0 4px;'
        'font-size:0.62rem;font-weight:700;letter-spacing:.08em;color:#475569;">SNS / NEWS</div>',
        unsafe_allow_html=True,
    )
    if st.button("💬　SNSモニタリング",
                 use_container_width=True, key="nav_sns"):
        st.session_state["active_section"] = "sns"
    if st.button("📰　停電ニュース",
                 use_container_width=True, key="nav_news"):
        st.session_state["active_section"] = "news"

    # ── 気象情報リンク ────────────────────────────────────
    st.markdown(
        '<div style="height:1px;background:#1e293b;margin:8px 0;"></div>'
        '<div style="font-size:0.65rem;font-weight:700;color:#475569;'
        'letter-spacing:0.08em;text-transform:uppercase;'
        'padding:10px 16px 4px;">Weather Links</div>',
        unsafe_allow_html=True,
    )
    _WEATHER_LINKS = [
        ("🚨", "気象警報・注意報",   "JMA",  "#dc2626",
         "https://www.jma.go.jp/bosai/warning/"),
        ("⚡", "落雷ナウキャスト",   "JMA",  "#ca8a04",
         "https://www.jma.go.jp/bosai/nowc/#elm=thunder"),
        ("🌧️", "高解像度降水ナウキャスト", "JMA", "#2563eb",
         "https://www.jma.go.jp/bosai/nowc/#elm=hrpns"),
        ("💨", "強風・突風情報",     "Windy","#0891b2",
         "https://www.windy.com/?wind,35,136,6"),
        ("🌩️", "雷・大雨（tenki）", "tenki","#7c3aed",
         "https://tenki.jp/radar/"),
        ("📡", "気象レーダー",       "JMA",  "#16a34a",
         "https://www.jma.go.jp/bosai/nowc/#elm=hrpns&contents=nowcast&layer=hrpns"),
    ]
    _links_html = ""
    for icon, label, badge, badge_color, url in _WEATHER_LINKS:
        _links_html += (
            f'<a class="weather-link" href="{url}" target="_blank" rel="noopener noreferrer">'
            f'{icon} {label}'
            f'<span class="wl-badge" style="background:{badge_color}22;color:{badge_color};">'
            f'{badge}</span>'
            f'</a>'
        )
    st.markdown(_links_html, unsafe_allow_html=True)

    # ── フッター ──────────────────────────────────────────
    st.markdown(
        '<div style="margin-top:16px;padding:10px 4px;'
        'border-top:1px solid #1e293b;'
        'font-size:0.68rem;color:#334155;text-align:center;">'
        '各電力ネットワーク会社公式サイトのデータを使用</div>',
        unsafe_allow_html=True,
    )


# ─── メインコンテンツ ─────────────────────────────────────────
section = st.session_state.get("active_section", "realtime")


# ═══════════════════════════════════════════════════
# リアルタイム停電情報
# ═══════════════════════════════════════════════════
if section == "realtime":
    with st.spinner("各電力ネットワーク会社から情報を取得中..."):
        df_rt = load_realtime_data()

    active  = df_rt[df_rt["affected_customers"] > 0]
    ok_cnt  = (df_rt["data_status"] == "取得済み").sum()
    ng_cnt  = (df_rt["data_status"] == "取得不可").sum()

    data_rate = ok_cnt / 47 * 100 if ok_cnt else 0
    total = int(active["affected_customers"].sum())
    k1, k2 = st.columns(2)
    with k1:
        st.markdown(f"""
        <div class="kpi-card red">
          <div class="kpi-head"><span class="kpi-icon">👥</span><span class="kpi-label">停電影響軒数（推定）</span></div>
          <div class="kpi-value">{total:,}<span class="unit">軒</span></div>
          <div class="kpi-sub">取得済み地域のみ</div>
        </div>""", unsafe_allow_html=True)
    with k2:
        st.markdown(f"""
        <div class="kpi-card orange">
          <div class="kpi-head"><span class="kpi-icon">📍</span><span class="kpi-label">停電中の都道府県数</span></div>
          <div class="kpi-value">{len(active)}<span class="unit"> / 47</span></div>
          <div class="kpi-sub">取得対象エリア</div>
        </div>""", unsafe_allow_html=True)
    st.markdown(
        f'<div style="font-size:0.78rem; color:#94a3b8; margin-bottom:0.5rem;">'
        f'取得率 {ok_cnt}/47都道府県（{data_rate:.0f}%）　更新 {now_str}'
        f'</div>',
        unsafe_allow_html=True,
    )

    confirmed_active = len(active)

    st.markdown(build_company_totals_html(df_rt), unsafe_allow_html=True)

    map_col, alert_col = st.columns([1.45, 0.95], gap="medium")
    with map_col:
        _overlay_col1, _overlay_col2, _overlay_col3 = st.columns(3)
        with _overlay_col1:
            show_radar = st.checkbox(
                "🌧️ 雨雲レーダー",
                value=False,
                key="map_show_radar",
                help="気象庁の高解像度降水ナウキャストを重ねて表示します。",
            )
        with _overlay_col2:
            show_typhoon = st.checkbox(
                "🌀 台風進路",
                value=False,
                key="map_show_typhoon",
                help="気象庁が発表中の台風・熱帯低気圧の進路と予報円を表示します。",
            )
        with _overlay_col3:
            show_lightning = st.checkbox(
                "⚡ 落雷ナウキャスト",
                value=False,
                key="map_show_lightning",
                help="気象庁の雷ナウキャスト（落雷位置・雷雲の強度）を重ねて表示します。",
            )

        radar_layer = load_jma_radar_layer() if show_radar else None
        typhoon_tracks = load_jma_typhoon_tracks() if show_typhoon else []
        lightning_layer = load_jma_lightning_layer() if show_lightning else None
        fig_map = build_japan_weather_map_fig(
            df_rt,
            radar_layer=radar_layer,
            typhoons=typhoon_tracks,
            lightning_layer=lightning_layer,
        )
        if fig_map is not None:
            _legend_items = "".join(
                f'<span><i style="background:{_MAP_COMPANY_STYLES[c][0]};'
                f'border:1px solid {_MAP_COMPANY_STYLES[c][1]};"></i>'
                f'{_short_company_name(c)}</span>'
                for c in _MAP_COMPANY_ORDER
            )
            st.markdown(
                '<div style="padding:6px 2px 2px;">'
                '<span style="font-size:.82rem;font-weight:700;color:#0f172a;">'
                '電力会社・地域別 停電状況マップ</span>'
                f'<div class="map-legend" style="margin-top:4px;">{_legend_items}</div>'
                '</div>',
                unsafe_allow_html=True,
            )
            _weather_status = []
            if show_radar:
                if radar_layer:
                    _weather_status.append(
                        f"🌧️ 雨雲 {radar_layer['observed_at']}現在（気象庁）"
                    )
                else:
                    _weather_status.append("🌧️ 雨雲データを取得できません")
            if show_typhoon:
                if typhoon_tracks is None:
                    _weather_status.append("🌀 台風進路データを取得できません")
                elif typhoon_tracks:
                    _weather_status.append(
                        f"🌀 台風・熱帯低気圧 {len(typhoon_tracks)}件（気象庁）"
                    )
                else:
                    _weather_status.append("🌀 現在、表示対象の台風情報はありません")
            if show_lightning:
                if lightning_layer:
                    _weather_status.append(
                        f"⚡ 落雷 {lightning_layer['observed_at']}現在（気象庁）"
                    )
                else:
                    _weather_status.append("⚡ 落雷データを取得できません")
            if _weather_status:
                st.caption("　|　".join(_weather_status))
            # 都道府県 → 電力会社URLのマッピング（クリックナビ用）
            _pref_url = {
                str(r["prefecture"]): _COMPANY_URLS.get(str(r.get("data_source", "")), "")
                for _, r in df_rt.iterrows()
                if _COMPANY_URLS.get(str(r.get("data_source", "")))
            }
            _pref_url_js = _json.dumps(_pref_url, ensure_ascii=False)
            _has_outages_js = "true" if confirmed_active > 0 else "false"

            # 停電軒数バッジ用データ（地理座標 + 軒数）
            _centroids = _compute_pref_centroids()
            _outage_centers = [
                {
                    "lon": _centroids[str(r["prefecture"])][0],
                    "lat": _centroids[str(r["prefecture"])][1],
                    "count": f"{int(r['affected_customers']):,}",
                    "n": int(r["affected_customers"]),
                }
                for _, r in df_rt.iterrows()
                if (
                    r.get("data_status") == "取得済み"
                    and r["affected_customers"] > 0
                    and str(r["prefecture"]) in _centroids
                )
            ]
            _outage_centers_js = _json.dumps(_outage_centers, ensure_ascii=False)

            _fig_html = _pio.to_html(
                fig_map,
                include_plotlyjs="cdn",
                full_html=True,
                config={"displayModeBar": False, "responsive": True},
            )
            _inject = f"""
<style>
  .map-btn{{
    width:32px;height:32px;border:1px solid #94a3b8;border-radius:6px;
    background:rgba(255,255,255,0.92);cursor:pointer;
    font-size:16px;font-weight:700;color:#334155;
    box-shadow:0 1px 4px rgba(0,0,0,0.18);line-height:1;
    display:flex;align-items:center;justify-content:center;
    transition:background 0.12s;
  }}
  .map-btn:hover{{background:rgba(241,245,249,0.97);}}
  .map-btn.blank{{background:transparent;border:none;box-shadow:none;pointer-events:none;}}
  .outage-badge{{
    position:absolute;
    border-radius:50%;
    background:transparent;
    display:flex;
    align-items:center;
    justify-content:center;
    font-weight:700;
    font-family:sans-serif;
    pointer-events:none;
    z-index:9998;
    transition:opacity 0.15s;
  }}
</style>
<div style="position:fixed;bottom:36px;right:12px;
            display:grid;grid-template-columns:32px 32px 32px;gap:4px;z-index:9999;">
  <div class="map-btn blank"></div>
  <button class="map-btn" id="btn-up"    title="上へ">▲</button>
  <div class="map-btn blank"></div>
  <button class="map-btn" id="btn-left"  title="左へ">◀</button>
  <div class="map-btn blank"></div>
  <button class="map-btn" id="btn-right" title="右へ">▶</button>
  <div class="map-btn blank"></div>
  <button class="map-btn" id="btn-down"  title="下へ">▼</button>
  <div class="map-btn blank"></div>
  <div class="map-btn blank"></div>
  <button class="map-btn" id="btn-zi"    title="拡大" style="font-size:20px;">+</button>
  <div class="map-btn blank"></div>
  <div class="map-btn blank"></div>
  <button class="map-btn" id="btn-zo"    title="縮小" style="font-size:20px;">−</button>
  <div class="map-btn blank"></div>
</div>
<script>
(function(){{
  var PREF_URL={_pref_url_js};
  var HAS_OUTAGES={_has_outages_js};
  var OUTAGE_CENTERS={_outage_centers_js};

  function mapboxInstance(){{
    var gd=document.querySelector('.plotly-graph-div');
    var subplot=gd&&gd._fullLayout&&gd._fullLayout.mapbox&&gd._fullLayout.mapbox._subplot;
    return subplot&&subplot.map?subplot.map:null;
  }}

  function zoomMap(delta){{
    var map=mapboxInstance();
    if(!map)return;
    map.easeTo({{zoom:Math.max(2,Math.min(10,map.getZoom()+delta)),duration:180}});
  }}

  function panMap(dlat, dlon){{
    var map=mapboxInstance();
    if(!map)return;
    var center=map.getCenter();
    var step=8/Math.pow(2,Math.max(0,map.getZoom()-3));
    map.easeTo({{center:[center.lng+dlon*step,center.lat+dlat*step],duration:180}});
  }}

  document.getElementById('btn-zi').addEventListener('click',function(){{zoomMap(0.7);}});
  document.getElementById('btn-zo').addEventListener('click',function(){{zoomMap(-0.7);}});
  document.getElementById('btn-up').addEventListener('click',function(){{panMap(1,0);}});
  document.getElementById('btn-down').addEventListener('click',function(){{panMap(-1,0);}});
  document.getElementById('btn-left').addEventListener('click',function(){{panMap(0,-1);}});
  document.getElementById('btn-right').addEventListener('click',function(){{panMap(0,1);}});

  var _badgesVisible=true;

  function sevColor(n){{
    if(n>=10000)return'#7f1d1d';
    if(n>=1000) return'#9a3412';
    if(n>=100)  return'#b45309';
    return'#78350f';
  }}

  function drawBadges(){{
    var gd=document.querySelector('.plotly-graph-div');
    var map=mapboxInstance();
    if(!gd||!map)return;
    gd.style.position='relative';
    gd.querySelectorAll('.outage-badge').forEach(function(el){{el.remove();}});
    if(!HAS_OUTAGES)return;
    var mc=gd.querySelector('.mapboxgl-map');
    var gdRect=gd.getBoundingClientRect();
    var mcRect=mc?mc.getBoundingClientRect():gdRect;
    var offX=mcRect.left-gdRect.left;
    var offY=mcRect.top-gdRect.top;
    OUTAGE_CENTERS.forEach(function(d){{
      var px=map.project([d.lon,d.lat]);
      var clen=d.count.length;
      var sz=Math.max(36,16+clen*8);
      var c=sevColor(d.n);
      var badge=document.createElement('div');
      badge.className='outage-badge';
      badge.textContent=d.count;
      badge.style.width=sz+'px';
      badge.style.height=sz+'px';
      badge.style.fontSize=Math.max(11,15-Math.max(0,clen-3))+'px';
      badge.style.border='2.5px solid '+c;
      badge.style.color=c;
      badge.style.textShadow='0 0 4px rgba(255,255,255,0.98),0 0 8px rgba(255,255,255,0.9)';
      badge.style.left=(offX+px.x-sz/2)+'px';
      badge.style.top=(offY+px.y-sz/2)+'px';
      badge.style.opacity=_badgesVisible?'1':'0.08';
      gd.appendChild(badge);
    }});
  }}

  function init(){{
    var gd=document.querySelector('.plotly-graph-div');
    if(!gd||!gd._fullLayout){{setTimeout(init,200);return;}}
    var map=mapboxInstance();
    if(!map){{setTimeout(init,200);return;}}

    gd.on('plotly_click',function(d){{
      if(!d.points||!d.points.length)return;
      var loc=d.points[0].location;
      if(loc&&PREF_URL[loc])window.open(PREF_URL[loc],'_blank','noopener,noreferrer');
    }});
    var s=document.createElement('style');
    s.textContent='.js-plotly-plot .mapboxgl-canvas{{cursor:pointer;}}';
    document.head.appendChild(s);

    map.on('move',drawBadges);
    map.on('zoom',drawBadges);
    map.on('resize',drawBadges);
    drawBadges();

    if(HAS_OUTAGES){{
      var blinkOn=true;
      var blinkTimer=setInterval(function(){{
        if(!document.body.contains(gd)){{clearInterval(blinkTimer);return;}}
        blinkOn=!blinkOn;
        _badgesVisible=blinkOn;
        // HTMLバッジを点滅
        gd.querySelectorAll('.outage-badge').forEach(function(el){{
          el.style.opacity=blinkOn?'1':'0.1';
        }});
        // Plotlyアラートオーバーレイ（trace 1）も同期点滅
        Plotly.restyle(gd,{{opacity:blinkOn?0.78:0.12}},[1]);
      }},700);
    }}
  }}
  setTimeout(init,800);
}})();
</script>"""
            _fig_html = _fig_html.replace("</body>", _inject + "</body>")
            _components.html(_fig_html, height=630, scrolling=False)
        else:
            st.markdown(build_prefecture_tile_map_html(df_rt), unsafe_allow_html=True)

    with alert_col:
        st.markdown(build_emergency_table_html(df_rt), unsafe_allow_html=True)


# ═══════════════════════════════════════════════════
# 停電原因データ（実データ）
# ═══════════════════════════════════════════════════
elif section == "cause":
    st.markdown(
        '<div class="section-title">⚠️ 停電原因データ</div>',
        unsafe_allow_html=True,
    )

    with st.spinner("停電履歴・起因データを取得中..."):
        df_hist_c = load_history_data()

    if df_hist_c.empty:
        st.warning("履歴データの取得に失敗しました。各社公式サイトをご参照ください。")
    else:
        covered = sorted(df_hist_c["company"].unique().tolist())
        covered_tags = "".join(
            f'<span class="coverage-tag tag-ok">✓ {c}</span>' for c in covered
        )
        ng_companies = ["中部電力パワーグリッド（起因なし）"]
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

        companies_c = ["全電力会社"] + sorted(df_hist_c["company"].unique().tolist())
        sel_company_c = st.radio(
            "電力会社（ワンクリックで絞り込み）", companies_c,
            horizontal=True, key="cause_company",
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

        st.markdown('<div class="section-title">日別 × 事故起因カテゴリー 停電件数の推移</div>',
                    unsafe_allow_html=True)
        if not dfc.empty:
            if "date_label" in dfc.columns and dfc["date_label"].ne("").any():
                group_col = "date_label"
                x_label   = "発生日"
            else:
                group_col = "month_label"
                x_label   = "月"
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

        st.markdown('<div class="section-title">事故起因別 実績サマリー</div>',
                    unsafe_allow_html=True)
        tbl = (
            dfc.groupby(["cause_category", "cause"])
            .agg(
                停電件数=("incidents",          "sum"),
                停電軒数=("affected_customers",  "sum"),
                停電時間=("total_outage_hours",  "sum"),
                対象都道府県数=("prefecture",    "nunique"),
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

        with st.expander("📋 生データ（停電記録一覧）を表示 ※列ヘッダーをクリックでソート",
                         expanded=False):
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
                use_container_width=True, height=400,
                column_config={
                    "発生日":       st.column_config.TextColumn("発生日",       width="small"),
                    "電力会社":     st.column_config.TextColumn("電力会社",     width="medium"),
                    "都道府県":     st.column_config.TextColumn("都道府県",     width="small"),
                    "地域":         st.column_config.TextColumn("地域",         width="small"),
                    "カテゴリー":   st.column_config.TextColumn("カテゴリー",   width="small"),
                    "起因（標準）": st.column_config.TextColumn("起因（標準）", width="medium"),
                    "起因（原文）": st.column_config.TextColumn("起因（原文）", width="large"),
                    "停電軒数":     st.column_config.NumberColumn("停電軒数",   format="%d 軒",   width="small"),
                    "停電時間(h)":  st.column_config.NumberColumn("停電時間(h)", format="%.2f h", width="small"),
                },
            )


# ═══════════════════════════════════════════════════
# 東北電力NW: リアルタイム（別枠）
# ═══════════════════════════════════════════════════
elif section == "tohoku_rt":
    st.markdown(
        '<div class="section-title">🏔️ 東北電力ネットワーク — リアルタイム停電状況</div>',
        unsafe_allow_html=True,
    )

    with st.spinner("東北電力ネットワークから情報を取得中..."):
        t_counts, t_ts = load_tohoku_realtime()

    st.markdown(
        f'<div style="font-size:0.8rem; color:#6b7280; margin-bottom:12px;">'
        f'情報更新: <b>{t_ts or "—"}</b>&ensp;|&ensp;'
        f'<a href="https://nw.tohoku-epco.co.jp/teideninfo/" target="_blank" style="color:#3b82f6">'
        f'東北電力ネットワーク 停電情報ページ</a></div>',
        unsafe_allow_html=True,
    )

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
    df_rt_t   = pd.DataFrame(rows_t)
    color_map = {"停電中": "#dc2626", "停電なし": "#16a34a", "取得不可": "#94a3b8"}
    fig_rt_bar = px.bar(
        df_rt_t, x="都道府県", y="停電軒数", color="状態",
        color_discrete_map=color_map, text="停電軒数",
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


# ═══════════════════════════════════════════════════
# 東北電力NW: 履歴分析（別枠）
# ═══════════════════════════════════════════════════
elif section == "tohoku_hist":
    st.markdown(
        '<div class="section-title">🏔️ 東北電力ネットワーク — 過去31日の履歴分析</div>',
        unsafe_allow_html=True,
    )

    with st.spinner("東北電力ネットワーク 過去31日データを取得中..."):
        df_th = load_tohoku_detail()

    if df_th.empty:
        st.warning("履歴データの取得に失敗しました。")
    else:
        prefs_sel  = ["全県"] + _TOHOKU_PREF_ORDER
        sel_pref_t = st.radio("都道府県（ワンクリック）", prefs_sel,
                              horizontal=True, key="tohoku_pref")

        fh2, fh3 = st.columns(2)
        with fh2:
            cats_t    = ["全カテゴリー"] + sorted(df_th["cause_category"].unique().tolist())
            sel_cat_t = st.selectbox("起因カテゴリー", cats_t, key="tohoku_cat")
        with fh3:
            weather_opts = ["全て（絞り込まない）"] + list(WEATHER_FLAG_CONFIG.keys())
            sel_weather  = st.selectbox("起因フラグ フィルター", weather_opts, key="tohoku_weather")

        dft = df_th.copy()
        if sel_pref_t != "全県":
            dft = dft[dft["pref_name"] == sel_pref_t]
        if sel_cat_t != "全カテゴリー":
            dft = dft[dft["cause_category"] == sel_cat_t]
        if sel_weather != "全て（絞り込まない）":
            dft = dft[dft["weather_flag"].str.contains(sel_weather, regex=False, na=False)]

        from collections import Counter as _Counter
        _all_flags: list[str] = []
        for _fs in dft["weather_flag"].fillna("不明"):
            _all_flags.extend(_fs.split("|"))
        wf_counts = _Counter(_all_flags)
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

        g1, g2 = st.columns([3, 2])
        with g1:
            st.markdown('<div class="section-title">日別 停電件数推移（起因フラグ別）</div>',
                        unsafe_allow_html=True)
            wcolor  = {k: v["badge_bg"] for k, v in WEATHER_FLAG_CONFIG.items()}
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

        st.markdown('<div class="section-title">各県別 停電トレンド（過去31日）</div>',
                    unsafe_allow_html=True)
        _W_COLORS = {k: v["badge_bg"] for k, v in WEATHER_FLAG_CONFIG.items()}

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

        st.markdown(
            '<div style="font-size:0.82rem; font-weight:700; color:#374151;'
            ' margin:14px 0 8px;">各県の日別件数（起因フラグ色分け）と集計サマリー</div>',
            unsafe_allow_html=True,
        )

        dft_all = df_th.copy()
        if sel_cat_t != "全カテゴリー":
            dft_all = dft_all[dft_all["cause_category"] == sel_cat_t]
        if sel_weather != "全て（絞り込まない）":
            dft_all = dft_all[dft_all["weather_flag"].str.contains(
                sel_weather, regex=False, na=False)]
        dft_all = dft_all.copy()
        dft_all["_primary_flag"] = dft_all["weather_flag"].str.split("|").str[0]

        sm_cols = st.columns(4)
        for p_idx, pref in enumerate(_TOHOKU_PREF_ORDER):
            pf = dft_all[dft_all["pref_name"] == pref]
            dot_color = _TOHOKU_PREF_COLOR.get(pref, "#6b7280")
            mini_data = (
                pf.groupby(["date_label", "_primary_flag"])["count"]
                .count().reset_index()
                .rename(columns={"count": "件数", "_primary_flag": "起因フラグ"})
                .sort_values("date_label")
            )
            total_inc  = len(pf)
            total_vol  = int(pf["count"].sum())
            nature_inc = pf["weather_flag"].apply(
                lambda x: any(f in str(x).split("|") for f in ["天候", "樹木・倒木"])
            ).sum()
            w_pct     = f"{nature_inc/total_inc*100:.0f}%" if total_inc > 0 else "—"
            top_cause = (
                pf[pf["raw_reason"].str.strip() != ""]["raw_reason"]
                .value_counts().index[0]
                if not pf[pf["raw_reason"].str.strip() != ""].empty else "—"
            )

            with sm_cols[p_idx % 4]:
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
                    fig_mini = px.bar(
                        mini_data, x="date_label", y="件数", color="起因フラグ",
                        color_discrete_map=_W_COLORS, barmode="stack",
                    )
                    fig_mini.update_layout(
                        height=130, margin=dict(t=2, b=2, l=2, r=2),
                        showlegend=False, plot_bgcolor="white", paper_bgcolor="white",
                    )
                    fig_mini.update_xaxes(showticklabels=False, showgrid=False, zeroline=False)
                    fig_mini.update_yaxes(showticklabels=True, gridcolor="#f3f4f6",
                                          tickformat=",d", nticks=3)
                    st.plotly_chart(fig_mini, use_container_width=True, key=f"mini_{pref}")

                    st.markdown(
                        f'<div style="background:#f8fafc; border-radius:6px;'
                        f' padding:7px 10px; font-size:0.73rem; margin-bottom:14px;">'
                        f'<div style="display:grid; grid-template-columns:1fr 1fr; gap:3px;">'
                        f'<div><span style="color:#6b7280;">件数</span> <b>{total_inc:,}</b></div>'
                        f'<div><span style="color:#6b7280;">軒数</span> <b>{total_vol:,}</b></div>'
                        f'<div><span style="color:#6b7280;">自然起因</span>'
                        f' <b style="color:#0369a1;">{w_pct}</b></div>'
                        f'<div style="grid-column:span 2; color:#6b7280; margin-top:2px;">'
                        f'主因: <b style="color:#374151;">{top_cause[:16]}</b></div>'
                        f'</div></div>',
                        unsafe_allow_html=True,
                    )

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

        st.markdown(
            '<div class="section-title">停電記録一覧 ※列ヘッダーをクリックでソート</div>',
            unsafe_allow_html=True,
        )
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
                "date_label":    "発生日",
                "pref_name":     "都道府県",
                "area_name":     "停電地域",
                "raw_reason":    "起因（原文）",
                "weather_flag":  "起因フラグ",
                "count":         "停電軒数",
                "start_time":    "発生時刻",
                "recovery_time": "復旧時刻",
                "duration_h":    "停電時間(h)",
            })
            .sort_values(["発生日", "都道府県"], ascending=[False, True])
            .reset_index(drop=True)
        )
        st.markdown(build_outage_table_html(disp_t), unsafe_allow_html=True)

        st.markdown(
            '<div style="font-size:0.75rem; color:#9ca3af; margin-top:8px;">'
            'データ取得元: '
            '<a href="https://nw.tohoku-epco.co.jp/teideninfo/rireki.html" '
            'target="_blank" style="color:#3b82f6">東北電力ネットワーク 停電履歴ページ</a>'
            '</div>',
            unsafe_allow_html=True,
        )


# ═══════════════════════════════════════════════════
# 各社詳細
# ═══════════════════════════════════════════════════
elif section.startswith("company_"):
    _comp_key = section[len("company_"):]
    _cfg = _COMPANY_DETAIL_CONFIG.get(_comp_key)
    if _cfg:
        st.markdown(
            f'<div class="section-title">🏢 {_cfg["company_name"]} — 詳細情報</div>',
            unsafe_allow_html=True,
        )
        with st.spinner(f"{_cfg['company_name']}のデータを取得中..."):
            _df_rt_comp   = load_realtime_data()
            _df_hist_comp = load_history_data()
        render_company_detail(
            company_name=_cfg["company_name"],
            pref_order=_cfg["pref_order"],
            pref_colors=_cfg["pref_colors"],
            rt_url=_cfg["rt_url"],
            hist_url=_cfg["hist_url"],
            df_rt=_df_rt_comp,
            df_hist=_df_hist_comp,
            key_prefix=_comp_key,
            n_hist_days=_cfg["n_hist_days"],
        )
    else:
        st.error(f"不明なセクション: {section}")


# ═══════════════════════════════════════════════════
# SNS情報（X）— 「停電」固定・シンプル版
# ═══════════════════════════════════════════════════
elif section == "sns":
    st.markdown(
        '<div class="section-title">🐦 X（旧Twitter）— 停電情報リアルタイム検索</div>',
        unsafe_allow_html=True,
    )

    _sns_query = "停電"
    _sns_url   = f"https://x.com/search?q={quote(_sns_query)}&f=live&src=typed_query"

    st.markdown(
        f"""
        <div style="background:#ffffff; border:1px solid #dbe3ee; border-radius:8px; padding:28px 32px;
             box-shadow:0 1px 2px rgba(15,23,42,0.04),0 8px 24px rgba(15,23,42,0.06); margin-top:8px; text-align:center;">
          <div style="font-size:1rem; font-weight:700; color:#172033; margin-bottom:6px;">
            「停電」キーワードで最新のXポストを確認
          </div>
          <div style="font-size:0.82rem; color:#64748b; margin-bottom:20px;">
            X（旧Twitter）は直接埋め込みに対応していないため、Xのサイトで最新順表示します
          </div>
          <a href="{_sns_url}" target="_blank" rel="noopener noreferrer"
             style="display:inline-block; background:#000; color:#fff;
                    font-size:1.05rem; font-weight:700; padding:14px 44px;
                    border-radius:999px; text-decoration:none; margin-bottom:16px;">
            𝕏 &nbsp;「停電」を最新順で見る
          </a>
          <div style="font-size:0.78rem; color:#94a3b8;">ライブ検索（新着順）が開きます</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        '<div style="font-size:0.88rem; font-weight:700; color:#172033;'
        ' margin:22px 0 10px;">エリア・原因別クイック検索</div>',
        unsafe_allow_html=True,
    )

    _quick_searches: list[tuple[str, str]] = [
        ("東北地方 停電",  "東北地方 停電"),
        ("宮城 停電",       "宮城 停電"),
        ("青森 停電",       "青森 停電"),
        ("岩手 停電",       "岩手 停電"),
        ("秋田 停電",       "秋田 停電"),
        ("山形 停電",       "山形 停電"),
        ("福島 停電",       "福島 停電"),
        ("新潟 停電",       "新潟 停電"),
        ("停電速報",        "停電速報"),
        ("地震 停電",       "地震 停電"),
        ("大雪 停電",       "大雪 停電"),
        ("落雷 停電",       "落雷 停電"),
        ("台風 停電",       "台風 停電"),
        ("停電 復旧",       "停電 復旧"),
    ]
    _btn_html = '<div style="display:flex; flex-wrap:wrap; gap:8px;">'
    for _lbl, _q in _quick_searches:
        _href = f"https://x.com/search?q={quote(_q)}&f=live&src=typed_query"
        _btn_html += (
            f'<a href="{_href}" target="_blank" rel="noopener noreferrer"'
            f' style="background:#ffffff; color:#334155; border-radius:999px;'
            f' padding:8px 18px; font-size:0.8rem; font-weight:600;'
            f' text-decoration:none; border:1px solid #dbe3ee;">'
            f'{_lbl}</a>'
        )
    _btn_html += '</div>'
    st.markdown(_btn_html, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════
# 停電ニュース — 自動取得・シンプル版
# ═══════════════════════════════════════════════════
elif section == "news":
    st.markdown(
        '<div class="section-title">📰 停電関連ニュース（Google ニュース RSS）</div>',
        unsafe_allow_html=True,
    )

    _news_q    = "停電"
    _gnews_url = f"https://news.google.com/search?q={quote(_news_q)}&hl=ja&gl=JP&ceid=JP:ja"
    st.link_button("🔗 Google ニュースで開く", _gnews_url)

    with st.spinner("ニュースを取得中..."):
        _news_items = load_news(_news_q)

    if not _news_items:
        st.warning("ニュースの取得に失敗しました。ネットワーク接続をご確認ください。", icon="⚠️")
    else:
        st.markdown(
            f'<div style="font-size:0.78rem; color:#6b7280; margin:8px 0 12px;">'
            f'最新 {len(_news_items)} 件（キャッシュ: 10分）</div>',
            unsafe_allow_html=True,
        )
        for _art in _news_items:
            _title_esc  = _html.escape(_art["title"])
            _source_esc = _html.escape(_art["source"])
            _pub_esc    = _html.escape(_art["pubDate"])
            _link       = _art["link"]
            st.markdown(
                f'<div style="background:#ffffff; border-radius:8px; padding:12px 16px;'
                f' margin-bottom:8px; box-shadow:0 1px 2px rgba(15,23,42,0.04),0 8px 24px rgba(15,23,42,0.06);'
                f' border:1px solid #dbe3ee; border-left:3px solid #2563eb;">'
                f'<a href="{_link}" target="_blank" rel="noopener noreferrer"'
                f' style="font-size:0.92rem; font-weight:600; color:#1d4ed8;'
                f' text-decoration:none;">{_title_esc}</a>'
                f'<div style="margin-top:5px; display:flex; gap:12px;'
                f' font-size:0.72rem; color:#6b7280;">'
                f'<span>📰 {_source_esc}</span>'
                f'<span>🕐 {_pub_esc}</span>'
                f'</div>'
                f'</div>',
                unsafe_allow_html=True,
            )


# ─── フッター ─────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    "<p style='text-align:center; color:#9ca3af; font-size:0.78rem;'>"
    "停電データ提供元: 各電力ネットワーク会社公式サイト"
    "（<a href='https://nw.tohoku-epco.co.jp/teideninfo/rireki.html' target='_blank' style='color:#3b82f6'>東北電力NW</a> / "
    "<a href='https://www.kansai-td.co.jp/teiden-info/index.php' target='_blank' style='color:#3b82f6'>関西電力送配電</a> / "
    "<a href='https://www.yonden.co.jp/nw/teiden-info/history.html' target='_blank' style='color:#3b82f6'>四国電力送配電</a> 他）"
    " | 参考: <a href='https://typhoon.yahoo.co.jp/weather/poweroutage/' target='_blank' style='color:#3b82f6'>"
    "Yahoo!天気・災害 停電情報</a>"
    " | <a href='https://www.fepc.or.jp/sp/bousai/link.html' target='_blank' style='color:#3b82f6'>"
    "電気事業連合会</a>"
    "</p>",
    unsafe_allow_html=True,
)
