"""
各電力ネットワーク会社ホームページからリアルタイム停電情報および
停電履歴（起因付き）を取得するモジュール。

リアルタイム取得可能:
  ・北海道電力ネットワーク ── HTML解析    (北海道・全道計)
  ・東北電力ネットワーク  ── JSON API    (青森/岩手/宮城/秋田/山形/福島/新潟)
  ・北陸電力送配電        ── HTML解析    (富山/石川/福井)
  ・中部電力パワーグリッド ── XML API     (愛知/三重/岐阜/静岡/長野)
  ・東京電力パワーグリッド ── XML API     (茨城/栃木/群馬/埼玉/千葉/東京/神奈川/山梨/静岡)
  ・関西電力送配電        ── JSON API    (滋賀/京都/大阪/兵庫/奈良/和歌山/福井/岐阜/三重)
  ・四国電力送配電        ── HTML解析    (香川/愛媛/徳島/高知)
  ・中国電力ネットワーク  ── HTML解析    (鳥取/島根/岡山/広島/山口)
  ・九州電力送配電        ── XML API     (福岡/佐賀/長崎/熊本/大分/宮崎/鹿児島)

履歴・起因データ取得可能:
  ・北海道電力ネットワーク ── HTML解析    (過去7日・起因付き)
  ・東北電力ネットワーク  ── JSON API    (過去31日・起因付き)
  ・北陸電力送配電        ── HTML解析    (過去7日・起因付き)
  ・東京電力パワーグリッド ── XML API     (過去60日・起因付き)
  ・関西電力送配電        ── JSON API    (過去7日・起因付き)
  ・四国電力送配電        ── HTML解析    (過去31日・件数のみ、起因なし)
  ・中部電力パワーグリッド ── 起因情報なし（毎正時スナップショットのみ公開）
  ・中国電力ネットワーク  ── HTML解析    (過去7日・起因付き)
  ・九州電力送配電        ── CSV API     (過去7日・起因付き)

取得不可（JS動的レンダリング・認証必要等）:
  ・沖縄電力（履歴のみ）
"""

import re
import time
import logging
from datetime import datetime, timedelta
from typing import Optional

import requests
from bs4 import BeautifulSoup
import pandas as pd

try:
    from .data_generator import PREFECTURES, CAUSE_CATEGORY
except ImportError:
    from data_generator import PREFECTURES, CAUSE_CATEGORY

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en;q=0.9",
}

# ── 管内都道府県マスタ ──────────────────────────────────────────
_TOHOKU_PREFS = {
    "02": "青森県", "03": "岩手県", "04": "宮城県",
    "05": "秋田県", "06": "山形県", "07": "福島県", "15": "新潟県",
}

# 関西電力のcodeorder（page.jsより）
_KANSAI_PREFS = {
    "27": "大阪府", "26": "京都府", "28": "兵庫県", "29": "奈良県",
    "30": "和歌山県", "25": "滋賀県", "18": "福井県", "21": "岐阜県", "24": "三重県",
}

_SHIKOKU_PREFS = ["香川県", "愛媛県", "徳島県", "高知県"]

# 東京電力パワーグリッド：エリアコード→都県名
_TEPCO_PREFS = {
    "08000000000": "茨城県",
    "09000000000": "栃木県",
    "10000000000": "群馬県",
    "11000000000": "埼玉県",
    "12000000000": "千葉県",
    "13000000000": "東京都",
    "14000000000": "神奈川県",
    "19000000000": "山梨県",
    "22000000000": "静岡県",
}

# 東京電力のCookie認証情報
_TEPCO_COOKIE_NAME  = "teideninfo-auth"
_TEPCO_COOKIE_VALUE = "sk3PT518"
_TEPCO_BASE_URL     = "https://teideninfo.tepco.co.jp"

# 沖縄電力
_OKINAWA_PREF     = "沖縄県"
_OKINAWA_BASE_URL = "https://www.okidenmail.jp"

# 中部電力パワーグリッド
_CHUBU_PREFS    = ["愛知県", "三重県", "岐阜県", "静岡県", "長野県"]
_CHUBU_BASE_URL = "https://teiden.powergrid.chuden.co.jp/p"

# 北陸電力送配電
_RIKUDEN_PREFS    = ["富山県", "石川県", "福井県"]
_RIKUDEN_BASE_URL = "https://www.rikuden.co.jp/nw/teiden"

# 中国電力ネットワーク
_CHUGOKU_PREFS    = ["鳥取県", "島根県", "岡山県", "広島県", "山口県"]
_CHUGOKU_BASE_URL = "https://www.teideninfo.energia.co.jp"

# 九州電力送配電
_KYUSHU_PREFS: dict[str, str] = {
    "40": "福岡県", "41": "佐賀県", "42": "長崎県",
    "43": "熊本県", "44": "大分県", "45": "宮崎県", "46": "鹿児島県",
}
_KYUSHU_BASE_URL = "https://www.kyuden.co.jp/td_teiden"

# ── 停電起因マッピング（各社の生テキスト → 標準起因名）─────────
_REASON_MAP: dict[str, str] = {
    # 北海道電力ネットワーク
    "設備の故障":                    "設備故障",
    "樹木等の接触":                  "樹木接触",
    "樹木等の接触（風雪の影響）":    "樹木接触",
    "樹木等の接触（風雨の影響）":    "樹木接触",
    "風雪の影響":                    "大雪・着雪",
    "風の影響":                      "強風",
    # 東北電力ネットワーク
    "氷雪の影響":               "大雪・着雪",
    "樹木接触・倒木の影響（氷雪）": "樹木接触",
    "樹木接触・倒木の影響（風雨）": "樹木接触",
    "樹木接触・倒木の影響":      "樹木接触",
    "風雨の影響":               "強風",
    "雷の影響":                 "落雷",
    "弊社設備の故障":           "設備故障",
    "車両衝突・接触等による影響": "交通事故（電柱衝突）",
    "近隣火災による影響":        "火災",
    "塩・ばい煙等の影響":        "塩害",
    "鳥獣・営巣等の影響":        "樹木接触",
    "お客さま設備故障の影響":    "その他",
    "第三者による過失等の影響":  "その他",
    "調査中":                   "不明",
    "調査の結果，特定できず":    "不明",
    "調査の結果、特定できず":    "不明",
    # 沖縄電力（理由文字列が既に標準起因名と同一の場合はpass-through）
    "設備故障":   "設備故障",
    "樹木接触":   "樹木接触",
    "鳥獣接触":   "その他",
    # 東京電力パワーグリッド
    "弊社設備のトラブル":                   "設備故障",
    "お客さま敷地内での設備トラブルの影響": "設備故障",
    "弊社設備への樹木等の接触":             "樹木接触",
    "弊社設備への雷の影響":                 "落雷",
    "弊社設備への風の影響":                 "強風",
    "弊社設備への雪の影響":                 "大雪・着雪",
    "弊社設備への塩害の影響":               "塩害",
    "車両等の衝突の影響":                   "交通事故（電柱衝突）",
    "火災の影響":                           "火災",
    "動物の接触の影響":                     "その他",
    "弊社設備への鳥獣の接触":              "その他",
    "原因調査中":                           "不明",
    # 関西電力送配電
    "弊社設備への樹木や鳥獣等の接触": "樹木接触",
    "弊社設備の不具合":               "設備故障",
    "弊社設備に対する作業中の不具合": "設備故障",
    "風や雨の影響":                   "強風",
    "お客さま設備の不具合による影響": "その他",
    "調査の結果、原因不明":           "不明",
    # 北陸電力送配電
    "鳥獣等の接触":         "その他",
    "樹木の接触・倒木":     "樹木接触",
    "雷による停電":         "落雷",
    "風雪・雪による停電":   "大雪・着雪",
    "強風による停電":       "強風",
    "設備の故障（電力）":   "設備故障",
    # 中国電力ネットワーク
    "当社設備の故障":               "設備故障",
    "当社設備への倒木":             "樹木接触",
    "倒木による当社設備の損傷":     "樹木接触",
    "当社設備への樹木の接触":       "樹木接触",
    "当社設備へのカラスの巣の接触": "その他",
    "当社設備への鳥獣の接触":       "その他",
    "風雨による当社設備の故障":     "強風",
    "風雪による当社設備の故障":     "大雪・着雪",
    "雪による当社設備の故障":       "大雪・着雪",
    "雷による当社設備の故障":       "落雷",
    "塩害による当社設備の故障":     "塩害",
    "車両等の衝突による当社設備の損傷": "交通事故（電柱衝突）",
    "火災による当社設備の損傷":     "火災",
    # 九州電力送配電 (弊社設備の故障 は東北と共通)
    "弊社設備の損傷（雷）":         "落雷",
    "弊社設備の損傷（風）":         "強風",
    "弊社設備の損傷（雪）":         "大雪・着雪",
    "弊社設備の損傷（倒木）":       "樹木接触",
    "弊社設備の損傷（樹木接触）":   "樹木接触",
    "弊社設備の損傷（塩害）":       "塩害",
    "弊社設備の損傷（鳥獣）":       "その他",
    "弊社設備の損傷（車両衝突）":   "交通事故（電柱衝突）",
    "弊社設備の損傷（火災）":       "火災",
}

_HOKKAIDO_PREF = "北海道"

_PREF_MASTER: dict[str, dict] = {p["name"]: p for p in PREFECTURES}

# ── 起因フラグ分類（複数フラグ対応） ──────────────────────────
# 各起因に対して「|」区切りで複数フラグを割り当てる。
#   "天候"    : 気象現象（氷雪・風雨・雷）が原因
#   "樹木・倒木": 樹木の接触・倒木が原因（天候誘因の有無を問わず）
#   "設備"    : 電力会社・顧客側設備の故障・不具合
#   "外的要因" : 車両衝突・火災・鳥獣など人為的/外部要因
#   "不明"    : 調査中・原因特定不能
_CAUSE_FLAGS_MAP: dict[str, list[str]] = {
    # ── 天候のみ ──────────────────────────────────────────────
    "氷雪の影響":                    ["天候"],
    "風雨の影響":                    ["天候"],
    "雷の影響":                      ["天候"],
    "風や雨の影響":                  ["天候"],        # 関西電力
    "風雪の影響":                    ["天候"],        # 北海道電力
    "風の影響":                      ["天候"],        # 北海道電力
    # ── 天候 + 樹木・倒木（複合） ────────────────────────────
    "樹木接触・倒木の影響（氷雪）":      ["天候", "樹木・倒木"],
    "樹木接触・倒木の影響（風雨）":      ["天候", "樹木・倒木"],
    "樹木等の接触（風雪の影響）":        ["天候", "樹木・倒木"],  # 北海道電力
    "樹木等の接触（風雨の影響）":        ["天候", "樹木・倒木"],  # 北海道電力
    # ── 樹木・倒木のみ ────────────────────────────────────────
    "樹木接触・倒木の影響":              ["樹木・倒木"],
    "樹木等の接触":                      ["樹木・倒木"],          # 北海道電力
    "弊社設備への樹木や鳥獣等の接触":   ["樹木・倒木"],          # 関西電力
    # ── 設備 ──────────────────────────────────────────────────
    "設備の故障":                        ["設備"],               # 北海道電力
    "弊社設備の故障":                    ["設備"],
    "弊社設備の不具合":                  ["設備"],               # 関西電力
    "弊社設備に対する作業中の不具合":    ["設備"],               # 関西電力
    "お客さま設備故障の影響":            ["設備"],
    "お客さま設備の不具合による影響":    ["設備"],               # 関西電力
    # ── 外的要因 ──────────────────────────────────────────────
    "車両衝突・接触等による影響":        ["外的要因"],
    "近隣火災による影響":                ["外的要因"],
    "第三者による過失等の影響":          ["外的要因"],
    "鳥獣・営巣等の影響":                ["外的要因"],
    "塩・ばい煙等の影響":                ["外的要因"],
    # ── 不明 ──────────────────────────────────────────────────
    "調査中":                            ["不明"],
    "調査の結果，特定できず":            ["不明"],
    "調査の結果、特定できず":            ["不明"],
    "調査の結果、原因不明":              ["不明"],               # 関西電力
    "原因調査中":                        ["不明"],               # 東京電力
    # ── 東京電力パワーグリッド ───────────────────────────────────
    "弊社設備のトラブル":                   ["設備"],
    "お客さま敷地内での設備トラブルの影響": ["設備"],
    "弊社設備への樹木等の接触":             ["樹木・倒木"],
    "弊社設備への雷の影響":                 ["天候"],
    "弊社設備への風の影響":                 ["天候"],
    "弊社設備への雪の影響":                 ["天候"],
    "弊社設備への塩害の影響":               ["外的要因"],
    "車両等の衝突の影響":                   ["外的要因"],
    "火災の影響":                           ["外的要因"],
    "動物の接触の影響":                     ["外的要因"],
    "弊社設備への鳥獣の接触":               ["外的要因"],
    # ── 沖縄電力 ──────────────────────────────────────────────────
    "樹木接触":   ["樹木・倒木"],
    "設備故障":   ["設備"],
    "鳥獣接触":   ["外的要因"],
    # ── 北陸電力送配電 ────────────────────────────────────────────
    "鳥獣等の接触":       ["外的要因"],
    "樹木の接触・倒木":   ["樹木・倒木"],
    "雷による停電":       ["天候"],
    "風雪・雪による停電": ["天候"],
    "強風による停電":     ["天候"],
    "設備の故障（電力）": ["設備"],
    # ── 中国電力ネットワーク ─────────────────────────────────────
    "当社設備の故障":               ["設備"],
    "当社設備への倒木":             ["樹木・倒木"],
    "倒木による当社設備の損傷":     ["樹木・倒木"],
    "当社設備への樹木の接触":       ["樹木・倒木"],
    "当社設備へのカラスの巣の接触": ["外的要因"],
    "当社設備への鳥獣の接触":       ["外的要因"],
    "風雨による当社設備の故障":     ["天候"],
    "風雪による当社設備の故障":     ["天候"],
    "雪による当社設備の故障":       ["天候"],
    "雷による当社設備の故障":       ["天候"],
    "塩害による当社設備の故障":     ["外的要因"],
    "車両等の衝突による当社設備の損傷": ["外的要因"],
    "火災による当社設備の損傷":     ["外的要因"],
    # ── 九州電力送配電 ───────────────────────────────────────────
    "弊社設備の損傷（雷）":       ["天候"],
    "弊社設備の損傷（風）":       ["天候"],
    "弊社設備の損傷（雪）":       ["天候"],
    "弊社設備の損傷（倒木）":     ["樹木・倒木"],
    "弊社設備の損傷（樹木接触）": ["樹木・倒木"],
    "弊社設備の損傷（塩害）":     ["外的要因"],
    "弊社設備の損傷（鳥獣）":     ["外的要因"],
    "弊社設備の損傷（車両衝突）": ["外的要因"],
    "弊社設備の損傷（火災）":     ["外的要因"],
}

# 起因フラグの表示ラベル・色設定（辞書順 = 表示順）
WEATHER_FLAG_CONFIG: dict[str, dict] = {
    "天候":      {"label": "🌧 天候",      "bg": "#dbeafe", "color": "#1e40af", "badge_bg": "#3b82f6"},
    "樹木・倒木": {"label": "🌲 樹木・倒木", "bg": "#dcfce7", "color": "#166534", "badge_bg": "#22c55e"},
    "設備":      {"label": "🔧 設備",      "bg": "#f3e8ff", "color": "#6b21a8", "badge_bg": "#a855f7"},
    "外的要因":   {"label": "🚗 外的要因",  "bg": "#fef3c7", "color": "#92400e", "badge_bg": "#f59e0b"},
    "不明":      {"label": "❓ 不明",      "bg": "#f1f5f9", "color": "#475569", "badge_bg": "#94a3b8"},
}


def _normalize_reason(raw: str) -> str:
    """生の起因テキストを標準起因名（CAUSE_CATEGORY のキー）にマッピング"""
    cleaned = raw.replace("\n", "").strip()
    return _REASON_MAP.get(cleaned, "その他")


def _classify_weather(raw: str) -> str:
    """生の起因テキストから起因フラグを返す（複数の場合は「|」区切り）。"""
    cleaned = raw.replace("\n", "").strip()
    flags = _CAUSE_FLAGS_MAP.get(cleaned, ["不明"])
    return "|".join(flags)


# ── 各社取得関数 ───────────────────────────────────────────────

def _tepco_session() -> requests.Session:
    """東京電力XMLアクセス用のCookie認証済みSessionを返す"""
    s = requests.Session()
    s.cookies.set(_TEPCO_COOKIE_NAME, _TEPCO_COOKIE_VALUE, domain="teideninfo.tepco.co.jp")
    return s


def fetch_tepco() -> tuple[dict[str, int], str]:
    ts_ms  = int(time.time() * 1000)
    xml_url = f"{_TEPCO_BASE_URL}/flash/xml/00000000000.xml?{ts_ms}"
    referer = f"{_TEPCO_BASE_URL}/"
    try:
        session = _tepco_session()
        resp = session.get(xml_url, headers={**_HEADERS, "Referer": referer}, timeout=12)
        resp.raise_for_status()
        resp.encoding = "utf-8"
        soup = BeautifulSoup(resp.text, "xml")

        ts_raw = soup.find("更新日時")
        ts = ""
        if ts_raw and ts_raw.text:
            raw = ts_raw.text.strip()  # e.g. "202603102029"
            try:
                dt = datetime.strptime(raw, "%Y%m%d%H%M")
                ts = dt.strftime("%Y年%m月%d日 %H:%M")
            except Exception:
                ts = raw

        result: dict[str, int] = {v: 0 for v in _TEPCO_PREFS.values()}
        for area in soup.find_all("エリア"):
            code  = area.get("コード", "")
            pref  = _TEPCO_PREFS.get(code)
            num   = area.find("停電軒数")
            if pref and num:
                try:
                    result[pref] = int(num.text.strip().replace(",", ""))
                except Exception:
                    pass
        return result, ts
    except Exception as exc:
        logger.warning("東京電力PG取得失敗: %s", exc)
        return {v: None for v in _TEPCO_PREFS.values()}, ""


def fetch_tepco_history(max_days: int = 31) -> list[dict]:
    """東京電力パワーグリッドの停電履歴（5分以上・過去最大60日）を取得"""
    ts_ms   = int(time.time() * 1000)
    referer = f"{_TEPCO_BASE_URL}/day/teiden/index-j.html"
    base    = f"{_TEPCO_BASE_URL}/day/teiden/"
    records: list[dict] = []

    try:
        session  = _tepco_session()
        hdrs     = {**_HEADERS, "Referer": referer}
        idx_resp = session.get(f"{base}index-j.xml?{ts_ms}", headers=hdrs, timeout=12)
        idx_resp.raise_for_status()
        idx_resp.encoding = "utf-8"
        idx_soup = BeautifulSoup(idx_resp.text, "xml")

        day_files: list[tuple[str, str]] = []
        for node in idx_soup.find_all("停電発生日"):
            file_name  = node.get("ファイル", "")
            date_text  = node.text.strip()  # e.g. "2026/03/10"
            if file_name and date_text:
                day_files.append((date_text, file_name))
        day_files = day_files[:max_days]

        for date_str, file_name in day_files:
            try:
                ts2    = int(time.time() * 1000)
                day_r  = session.get(
                    f"{base}{file_name}?{ts2}",
                    headers={**_HEADERS, "Referer": referer},
                    timeout=10,
                )
                day_r.encoding = "utf-8"
                day_soup = BeautifulSoup(day_r.text, "xml")

                for sel in day_soup.find_all("停電表示選択"):
                    if "５分以上" not in sel.get("値", ""):
                        continue
                    for block in sel.find_all("データ部"):
                        start_raw    = (block.find("発生日時") or {}).text if block.find("発生日時") else ""
                        end_raw      = (block.find("復旧日時") or {}).text if block.find("復旧日時") else ""
                        count_node   = block.find("停電軒数")
                        reason_node  = block.find("停電理由")
                        count        = int(count_node.text.strip().replace(",", "")) if count_node else 0
                        raw_reason   = reason_node.text.strip() if reason_node else ""

                        for pref_node in block.find_all("都県部"):
                            pref_name = (pref_node.find("都県名") or {}).text if pref_node.find("都県名") else ""
                            if not pref_name:
                                continue
                            records.append({
                                "date":          date_str.replace("/", "-"),
                                "pref_name":     pref_name.strip(),
                                "area_name":     "",
                                "count":         count,
                                "raw_reason":    raw_reason,
                                "cause":         _normalize_reason(raw_reason),
                                "weather_flag":  _classify_weather(raw_reason),
                                "company":       "東京電力パワーグリッド",
                                "start_time":    start_raw.strip(),
                                "recovery_time": end_raw.strip(),
                            })
            except Exception as exc:
                logger.debug("東京電力PG日別履歴取得失敗 %s: %s", file_name, exc)
    except Exception as exc:
        logger.warning("東京電力PG履歴取得失敗: %s", exc)
    return records


def fetch_chubu() -> tuple[dict[str, int], str]:
    """中部電力パワーグリッドのリアルタイム停電情報を取得（愛知/三重/岐阜/静岡/長野）"""
    ts_ms     = int(time.time() * 1000)
    index_url = f"{_CHUBU_BASE_URL}/resource/disclose/xml/index.xml?{ts_ms}"
    area_url  = f"{_CHUBU_BASE_URL}/resource/xml/teiden_area.xml?{ts_ms}"
    referer   = f"{_CHUBU_BASE_URL}/index.html"
    hdrs      = {**_HEADERS, "Referer": referer}
    result: dict[str, int] = {p: 0 for p in _CHUBU_PREFS}
    ts = ""
    try:
        area_r = requests.get(area_url, headers=hdrs, timeout=10)
        area_r.encoding = "utf-8"
        area_soup = BeautifulSoup(area_r.text, "xml")
        dt_node = area_soup.find("data_make_d")
        if dt_node and dt_node.text:
            raw = dt_node.text.strip()  # e.g. "2026/03/10 20:41"
            try:
                dt = datetime.strptime(raw, "%Y/%m/%d %H:%M")
                ts = dt.strftime("%Y年%m月%d日 %H:%M")
            except Exception:
                ts = raw

        idx_r = requests.get(index_url, headers=hdrs, timeout=10)
        idx_r.raise_for_status()
        idx_r.encoding = "utf-8"
        idx_soup = BeautifulSoup(idx_r.text, "xml")
        for area in idx_soup.find_all("area"):
            addr_node = area.find("address")
            kosu_node = area.find("genzai_teiden_kosu")
            if addr_node and kosu_node:
                pref = addr_node.text.strip()
                if pref in result:
                    try:
                        result[pref] = int(kosu_node.text.strip().replace(",", ""))
                    except Exception:
                        pass
        return result, ts
    except Exception as exc:
        logger.warning("中部電力PG取得失敗: %s", exc)
        return {p: None for p in _CHUBU_PREFS}, ""


# ── 北陸電力送配電 ────────────────────────────────────────────────────────────

def fetch_rikuden() -> tuple[dict[str, int], str]:
    """北陸電力送配電のリアルタイム停電情報を取得（富山/石川/福井）"""
    url = f"{_RIKUDEN_BASE_URL}/otj010.html"
    result: dict[str, int] = {p: 0 for p in _RIKUDEN_PREFS}
    try:
        r = requests.get(url, headers=_HEADERS, timeout=12)
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text(separator="\n", strip=True)

        ts = ""
        m = re.search(r"(\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2})\s*現在", text)
        if m:
            try:
                dt = datetime.strptime(m.group(1), "%Y/%m/%d %H:%M")
                ts = dt.strftime("%Y年%m月%d日 %H:%M")
            except Exception:
                ts = m.group(1)

        if "停電は発生しておりません" in text:
            return result, ts

        # 停電発生中の場合: メインページから件数を推測する
        # （件数の記載なしのためすべて None 扱い）
        return {p: None for p in _RIKUDEN_PREFS}, ts
    except Exception as exc:
        logger.warning("北陸電力NW取得失敗: %s", exc)
        return {p: None for p in _RIKUDEN_PREFS}, ""


def fetch_rikuden_history() -> list[dict]:
    """北陸電力送配電の過去7日分の停電履歴（起因付き）を取得"""
    today_str = datetime.now().strftime("%Y%m%d")
    url = f"{_RIKUDEN_BASE_URL}/f1/sevendays/{today_str}/otj600.html"
    records: list[dict] = []
    try:
        r = requests.get(url, headers=_HEADERS, timeout=15)
        if r.status_code != 200:
            return records
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")
        for tbl in soup.find_all("table"):
            rows = tbl.find_all("tr")
            for row in rows:
                cells = [td.get_text(separator=" ", strip=True) for td in row.find_all(["td", "th"])]
                if len(cells) < 7:
                    continue
                start_raw  = cells[0]
                end_raw    = cells[1]
                pref_name  = cells[2]
                count_raw  = cells[5]
                reason_raw = cells[6]
                # 自動復旧等（5分未満）はスキップ
                if "自動復旧等" in count_raw or reason_raw in ("―", "-"):
                    continue
                if pref_name not in _RIKUDEN_PREFS:
                    continue
                count = 0
                m = re.search(r"[\d,]+", count_raw.replace(",", ""))
                if m:
                    try:
                        count = int(m.group(0).replace(",", ""))
                    except Exception:
                        pass
                try:
                    dt_start = datetime.strptime(start_raw, "%Y/%m/%d %H:%M")
                except Exception:
                    continue
                records.append({
                    "date":          dt_start.strftime("%Y-%m-%d"),
                    "pref_name":     pref_name,
                    "count":         count,
                    "raw_reason":    reason_raw.strip(),
                    "cause":         _normalize_reason(reason_raw.strip()),
                    "company":       "北陸電力送配電",
                    "start_time":    dt_start.strftime("%Y-%m-%d %H:%M"),
                    "recovery_time": "",
                })
    except Exception as exc:
        logger.warning("北陸電力NW履歴取得失敗: %s", exc)
    return records


# ── 中国電力ネットワーク ──────────────────────────────────────────────────────

def fetch_chugoku() -> tuple[dict[str, int], str]:
    """中国電力ネットワークのリアルタイム停電情報を取得（鳥取/島根/岡山/広島/山口）"""
    url = _CHUGOKU_BASE_URL + "/"
    result: dict[str, int] = {p: 0 for p in _CHUGOKU_PREFS}
    try:
        r = requests.get(url, headers={**_HEADERS, "Referer": url}, timeout=12)
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text(separator="\n", strip=True)

        ts = ""
        m = re.search(r"最終更新日時[：:]\s*(\d{4}年\d{2}月\d{2}日\s+\d{2}:\d{2})", text)
        if m:
            ts = m.group(1).strip()

        # 停電なし確認
        if "停電はありません" in text:
            return result, ts

        # 停電発生中: js-tdk属性（data-tdk）で県別件数を確認
        for li in soup.find_all("li", attrs={"data-tdk": True}):
            pref = li["data-tdk"].strip()
            if pref in result:
                result[pref] = result.get(pref, 0) + 1
        return result, ts
    except Exception as exc:
        logger.warning("中国電力NW取得失敗: %s", exc)
        return {p: None for p in _CHUGOKU_PREFS}, ""


def fetch_chugoku_history() -> list[dict]:
    """中国電力ネットワークの過去7日分の停電履歴（起因付き）を取得"""
    base = _CHUGOKU_BASE_URL + "/LWC30040/index"
    referer = _CHUGOKU_BASE_URL + "/LWC30040"
    hdrs = {**_HEADERS, "Referer": referer}
    records: list[dict] = []
    today = datetime.now()
    for days_back in range(8):
        dt = today - timedelta(days=days_back)
        date_str = dt.strftime("%Y%m%d")
        try:
            r = requests.get(base, params={"date": date_str, "type": "1"},
                             headers=hdrs, timeout=15)
            if r.status_code != 200:
                continue
            r.encoding = "utf-8"
            soup = BeautifulSoup(r.text, "html.parser")
            text = soup.get_text()
            if "停電履歴情報はありません" in text:
                continue

            # 各インシデント: <ul class="js-knm p-table-list">
            for ul in soup.find_all("ul", class_="js-knm"):
                li_items = ul.find_all("li", recursive=False)
                # 2番目の<li>（インデックス1）がデータ行
                data_li = None
                for li in li_items:
                    if "p-table-list_head" not in li.get("class", []):
                        if data_li is None:
                            data_li = li
                if not data_li:
                    continue

                divs = data_li.find_all("div", recursive=False)
                if len(divs) < 4:
                    continue
                start_raw  = divs[0].get_text(separator=" ", strip=True)
                end_raw    = divs[1].get_text(separator=" ", strip=True)
                reason_raw = divs[2].get_text(strip=True)
                count_raw  = divs[3].get_text(strip=True)

                start_raw = re.sub(r"\s+", "", start_raw)
                try:
                    dt_start = datetime.strptime(start_raw, "%Y/%m/%d%H:%M")
                except Exception:
                    continue

                count = 0
                m = re.search(r"[\d,]+", count_raw)
                if m:
                    try:
                        count = int(m.group(0).replace(",", ""))
                    except Exception:
                        pass

                # 県名を収集 (data-tdk)
                prefs_in_incident = set()
                for li_pref in ul.find_all("li", attrs={"data-tdk": True}):
                    pref = li_pref["data-tdk"].strip()
                    if pref in _CHUGOKU_PREFS:
                        prefs_in_incident.add(pref)

                for pref in prefs_in_incident:
                    records.append({
                        "date":          dt_start.strftime("%Y-%m-%d"),
                        "pref_name":     pref,
                        "count":         count,
                        "raw_reason":    reason_raw,
                        "cause":         _normalize_reason(reason_raw),
                        "company":       "中国電力ネットワーク",
                        "start_time":    dt_start.strftime("%Y-%m-%d %H:%M"),
                        "recovery_time": "",
                    })
        except Exception as exc:
            logger.warning("中国電力NW履歴取得失敗 %s: %s", date_str, exc)
    return records


# ── 九州電力送配電 ────────────────────────────────────────────────────────────

def fetch_kyushu() -> tuple[dict[str, int], str]:
    """九州電力送配電のリアルタイム停電情報を取得（福岡/佐賀/長崎/熊本/大分/宮崎/鹿児島）"""
    import time as _time
    ts_ms = int(_time.time() * 1000)
    url = f"{_KYUSHU_BASE_URL}/xml/00.xml?{ts_ms}"
    referer = f"{_KYUSHU_BASE_URL}/kyushu.html"
    hdrs = {**_HEADERS, "Referer": referer}
    result: dict[str, int] = {v: 0 for v in _KYUSHU_PREFS.values()}
    ts = ""
    try:
        r = requests.get(url, headers=hdrs, timeout=12)
        r.raise_for_status()
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "xml")
        header = soup.find("HEADER")
        if header:
            rel = header.find("RELEASE_DATE")
            if rel and rel.text:
                raw = rel.text.strip()  # "20260310210712"
                try:
                    dt = datetime.strptime(raw, "%Y%m%d%H%M%S")
                    ts = dt.strftime("%Y年%m月%d日 %H:%M")
                except Exception:
                    ts = raw
        for data in soup.find_all("DATA"):
            pref_id   = (data.find("PREF_ID")   or type("", (), {"text": ""})()).text.strip()
            pref_name = (data.find("PREF_NAME")  or type("", (), {"text": ""})()).text.strip()
            count_raw = (data.find("BLACKOUT_COUNT") or type("", (), {"text": "0戸"})()).text.strip()
            if pref_name not in result:
                continue
            m = re.search(r"[\d,]+", count_raw)
            if m:
                try:
                    result[pref_name] = int(m.group(0).replace(",", ""))
                except Exception:
                    pass
        return result, ts
    except Exception as exc:
        logger.warning("九州電力送配電取得失敗: %s", exc)
        return {v: None for v in _KYUSHU_PREFS.values()}, ""


def fetch_kyushu_history() -> list[dict]:
    """九州電力送配電の過去7日分の停電履歴（起因付き）を取得"""
    today = datetime.now()
    ts_suf = str(int(time.time() * 1000) // 100000)
    referer = f"{_KYUSHU_BASE_URL}/rireki.html"
    hdrs = {**_HEADERS, "Referer": referer}
    records: list[dict] = []
    for pref_id, pref_name in _KYUSHU_PREFS.items():
        for days_back in range(8):
            dt = today - timedelta(days=days_back)
            date_str = dt.strftime("%Y%m%d")
            url = f"{_KYUSHU_BASE_URL}/csv/RES{pref_id}_{date_str}.csv?{ts_suf}"
            time.sleep(0.3)
            try:
                r = requests.get(url, headers=hdrs, timeout=12)
                if r.status_code != 200 or not r.text.strip():
                    continue
                r.encoding = "utf-8"
                seen: dict[tuple, dict] = {}
                for line in r.text.strip().split("\n"):
                    cols = line.strip().split(",")
                    if len(cols) < 7:
                        continue
                    start_raw = cols[0].strip()
                    reason_raw = cols[6].strip()
                    count_raw  = cols[4].strip()
                    try:
                        dt_start = datetime.strptime(start_raw, "%Y%m%d%H%M")
                    except Exception:
                        continue
                    count = 0
                    try:
                        count = int(count_raw.replace(",", ""))
                    except Exception:
                        pass
                    key = (start_raw, reason_raw)
                    if key not in seen:
                        seen[key] = {"count": count, "dt_start": dt_start, "reason": reason_raw}
                    else:
                        seen[key]["count"] = max(seen[key]["count"], count)
                for val in seen.values():
                    records.append({
                        "date":          val["dt_start"].strftime("%Y-%m-%d"),
                        "pref_name":     pref_name,
                        "count":         val["count"],
                        "raw_reason":    val["reason"],
                        "cause":         _normalize_reason(val["reason"]),
                        "company":       "九州電力送配電",
                        "start_time":    val["dt_start"].strftime("%Y-%m-%d %H:%M"),
                        "recovery_time": "",
                    })
            except Exception as exc:
                logger.warning("九州電力送配電履歴取得失敗 %s %s: %s", pref_id, date_str, exc)
    return records


def fetch_hokkaido() -> tuple[dict[str, int], str]:
    url = "https://teiden-info.hepco.co.jp/"
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=12)
        resp.raise_for_status()
        resp.encoding = "utf-8"
        soup = BeautifulSoup(resp.text, "html.parser")

        ts = ""
        for el in soup.find_all(string=re.compile(r"\d+月\d+日.*現在")):
            ts = el.strip()
            break

        count = 0
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            for row in rows:
                cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                if "全道計" in cells:
                    idx = cells.index("全道計")
                    if idx + 1 < len(cells):
                        nums = re.findall(r"[\d,]+", cells[idx + 1])
                        if nums:
                            count = int(nums[0].replace(",", ""))

        return {_HOKKAIDO_PREF: count}, ts
    except Exception as exc:
        logger.warning("北海道電力NW取得失敗: %s", exc)
        return {_HOKKAIDO_PREF: None}, ""


def fetch_hokkaido_history() -> list[dict]:
    url = "https://teiden-info.hepco.co.jp/past00000000.html"
    records: list[dict] = []
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=12)
        resp.raise_for_status()
        resp.encoding = "utf-8"
        soup = BeautifulSoup(resp.text, "html.parser")

        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            header_cells = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])] if rows else []
            if "停電原因" not in " ".join(header_cells):
                continue
            for row in rows[1:]:
                cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                if len(cells) < 4:
                    continue
                time_cell  = cells[0]
                address    = cells[1]
                count_raw  = cells[2]
                reason_raw = cells[3].strip()

                m = re.match(
                    r"(\d{4}/\d{2}/\d{2})(\d{2}:\d{2}).*?(\d{4}/\d{2}/\d{2})(\d{2}:\d{2})",
                    time_cell,
                )
                if not m:
                    continue
                start_date  = m.group(1)
                start_time  = m.group(1) + " " + m.group(2)
                end_time    = m.group(3) + " " + m.group(4)

                nums  = re.findall(r"[\d,]+", count_raw)
                count = int(nums[0].replace(",", "")) if nums else 0

                records.append({
                    "date":          start_date,
                    "pref_name":     _HOKKAIDO_PREF,
                    "area_name":     address,
                    "count":         count,
                    "raw_reason":    reason_raw,
                    "cause":         _normalize_reason(reason_raw),
                    "weather_flag":  _classify_weather(reason_raw),
                    "company":       "北海道電力ネットワーク",
                    "start_time":    start_time,
                    "recovery_time": end_time,
                })
    except Exception as exc:
        logger.warning("北海道電力履歴取得失敗: %s", exc)
    return records


def fetch_tohoku() -> tuple[dict[str, int], str]:
    url = "https://nw.tohoku-epco.co.jp/teideninfo/blackout/top.json"
    try:
        resp = requests.get(
            url,
            headers={**_HEADERS, "Referer": "https://nw.tohoku-epco.co.jp/teideninfo/"},
            timeout=12,
        )
        resp.raise_for_status()
        data = resp.json()
        result = {}
        for item in data.get("details", []):
            name  = item.get("name", "")
            count = item.get("count", 0) or 0
            if name:
                result[name] = int(count)
        for pref in _TOHOKU_PREFS.values():
            result.setdefault(pref, 0)
        ts = data.get("time", "")
        return result, ts
    except Exception as exc:
        logger.warning("東北電力NW取得失敗: %s", exc)
        return {p: None for p in _TOHOKU_PREFS.values()}, ""


def fetch_kansai() -> tuple[dict[str, int], str]:
    url = "https://www.kansai-td.co.jp/interchange/teiden-info/ja/alert.json"
    try:
        resp = requests.get(
            url,
            headers={**_HEADERS, "Referer": "https://www.kansai-td.co.jp/teiden-info/index.php"},
            timeout=12,
        )
        resp.raise_for_status()
        data = resp.json()
        result: dict[str, int] = {}

        def _traverse(items: list) -> None:
            for item in items:
                name  = item.get("name", "")
                count = item.get("count", item.get("number", 0)) or 0
                if any(name.endswith(sfx) for sfx in ("府", "県", "道", "都")):
                    result[name] = int(count)
                if item.get("children"):
                    _traverse(item["children"])

        _traverse(data.get("children", []))
        if not result:
            result = {p: 0 for p in _KANSAI_PREFS.values()}
        ts = data.get("datetime", "")
        return result, ts
    except Exception as exc:
        logger.warning("関西電力送配電取得失敗: %s", exc)
        return {p: None for p in _KANSAI_PREFS.values()}, ""


def fetch_shikoku() -> tuple[dict[str, int], str]:
    url = "https://www.yonden.co.jp/nw/teiden-info/index.html"
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=12)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        lines = [ln.strip() for ln in soup.get_text(separator="\n").split("\n") if ln.strip()]
        result: dict[str, int] = {}
        ts = ""
        for ln in lines:
            if re.search(r"\d{4}年\d+月\d+日", ln) and "現在" in ln:
                ts = ln
                break
        for i, line in enumerate(lines):
            for pref in _SHIKOKU_PREFS:
                if line == pref and i + 1 < len(lines):
                    nxt = lines[i + 1]
                    if "停電情報はありません" in nxt:
                        result[pref] = 0
                    else:
                        nums = re.findall(r"(\d[\d,]*)\s*軒", nxt)
                        result[pref] = int(nums[0].replace(",", "")) if nums else 0
        for pref in _SHIKOKU_PREFS:
            result.setdefault(pref, 0)
        return result, ts
    except Exception as exc:
        logger.warning("四国電力送配電取得失敗: %s", exc)
        return {p: None for p in _SHIKOKU_PREFS}, ""


def fetch_okinawa() -> tuple[dict[str, int], str]:
    url = f"{_OKINAWA_BASE_URL}/bosai/api/xml_map_koazaBetsu.php"
    hdrs = {**_HEADERS, "Referer": f"{_OKINAWA_BASE_URL}/bosai/info/index.html"}
    try:
        resp = requests.get(url, headers=hdrs, timeout=12)
        resp.raise_for_status()
        resp.encoding = "utf-8"
        soup = BeautifulSoup(resp.text, "xml")

        ts = ""
        dt_node = soup.find("datetime")
        if dt_node:
            ts = dt_node.text.strip()

        count = 0
        house_node = soup.find("pref_power_cut_house")
        if house_node:
            try:
                count = int(house_node.text.strip().replace(",", ""))
            except Exception:
                pass
        return {_OKINAWA_PREF: count}, ts
    except Exception as exc:
        logger.warning("沖縄電力取得失敗: %s", exc)
        return {_OKINAWA_PREF: None}, ""


def fetch_okinawa_history() -> list[dict]:
    """沖縄電力の過去停電履歴（直近数件・起因付き）を取得"""
    hdrs_info2 = {**_HEADERS, "Referer": f"{_OKINAWA_BASE_URL}/bosai/info2"}
    records: list[dict] = []
    try:
        # 日付キー一覧を取得
        r = requests.get(
            f"{_OKINAWA_BASE_URL}/bosai/xml/history_normal.xml",
            headers=hdrs_info2, timeout=12,
        )
        r.raise_for_status()
        r.encoding = "utf-8"
        idx_soup = BeautifulSoup(r.text, "xml")

        date_keys: list[tuple[str, str]] = []
        for item in idx_soup.find_all("history_item"):
            title_node = item.find("title")
            dk_node    = item.find("date_key")
            if title_node and dk_node:
                date_keys.append((title_node.text.strip(), dk_node.text.strip()))

        for title, dk in date_keys:
            try:
                r2 = requests.get(
                    f"{_OKINAWA_BASE_URL}/bosai/api/xml_map2.php",
                    headers=hdrs_info2,
                    params={"date_key": dk},
                    timeout=10,
                )
                r2.encoding = "utf-8"
                s2 = BeautifulSoup(r2.text, "xml")

                # date_key の先頭8文字 (YYYYMMDD) を日付に変換
                date_str = dk[:8]
                try:
                    date_str = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
                except Exception:
                    pass

                for town in s2.find_all("town"):
                    raw_reason = (town.find("reason") or {}).text if town.find("reason") else ""
                    raw_reason = raw_reason.strip() if isinstance(raw_reason, str) else ""

                    # 過去履歴では restoration_house（復旧戸数）= 発生時停電戸数
                    resto_house_node  = town.find("restoration_house")
                    start_house_node  = town.find("power_cut_start_house")
                    count_val = 0
                    for node in [resto_house_node, start_house_node]:
                        if node:
                            val = node.text.strip().replace(",", "")
                            if val.isdigit() and int(val) > 0:
                                count_val = int(val)
                                break

                    pc_date = (town.find("power_cut_date") or {}).text if town.find("power_cut_date") else date_str
                    pc_time = (town.find("power_cut_time") or {}).text if town.find("power_cut_time") else ""
                    en_date = (town.find("send_finish_date") or {}).text if town.find("send_finish_date") else ""
                    en_time = (town.find("send_finish_time") or {}).text if town.find("send_finish_time") else ""

                    start_dt = f"{pc_date} {pc_time}".strip() if isinstance(pc_date, str) else ""
                    end_dt   = f"{en_date} {en_time}".strip() if isinstance(en_date, str) else ""

                    records.append({
                        "date":          date_str,
                        "pref_name":     _OKINAWA_PREF,
                        "area_name":     town.get("name", ""),
                        "count":         count_val,
                        "raw_reason":    raw_reason,
                        "cause":         _normalize_reason(raw_reason),
                        "weather_flag":  _classify_weather(raw_reason),
                        "company":       "沖縄電力",
                        "start_time":    start_dt,
                        "recovery_time": end_dt,
                    })
            except Exception as exc:
                logger.debug("沖縄電力日別履歴取得失敗 %s: %s", dk, exc)
    except Exception as exc:
        logger.warning("沖縄電力履歴取得失敗: %s", exc)
    return records


# ── 履歴・起因取得関数 ────────────────────────────────────────

def fetch_tohoku_history() -> list[dict]:
    base    = "https://nw.tohoku-epco.co.jp/teideninfo/blackout/"
    referer = "https://nw.tohoku-epco.co.jp/teideninfo/rireki.html"
    hdrs    = {**_HEADERS, "Referer": referer}
    records: list[dict] = []
    try:
        top = requests.get(base + "rirekiinfo_top.json", headers=hdrs, timeout=12).json()
        for date_item in top.get("dates", []):
            file_no  = date_item["file_no"]
            date_str = date_item["date"]
            try:
                data = requests.get(
                    base + f"rirekiinfo{file_no}.json", headers=hdrs, timeout=10,
                ).json()
                for d in data.get("details", []):
                    raw = d.get("reason", "不明").replace("\n", "")
                    records.append({
                        "date":          date_str,
                        "pref_code":     d.get("pref_code", ""),
                        "pref_name":     d.get("pref_name", ""),
                        "area_name":     d.get("name", "").replace("\n", " "),
                        "count":         int(d.get("count", 0) or 0),
                        "raw_reason":    raw,
                        "cause":         _normalize_reason(raw),
                        "weather_flag":  _classify_weather(raw),
                        "company":       "東北電力ネットワーク",
                        "start_time":    d.get("time", ""),
                        "recovery_time": d.get("recovery_time", ""),
                    })
            except Exception:
                pass
    except Exception as exc:
        logger.warning("東北電力履歴取得失敗: %s", exc)
    return records


def fetch_kansai_history() -> list[dict]:
    url     = "https://www.kansai-td.co.jp/interchange/teiden-info/ja/history.json"
    referer = "https://www.kansai-td.co.jp/teiden-info/index.php"
    records: list[dict] = []
    try:
        data = requests.get(url, headers={**_HEADERS, "Referer": referer}, timeout=12).json()
        for day in data.get("list", []):
            for item in day.get("list", []):
                raw      = item.get("offcause", "不明")
                offtime  = item.get("offdatetime", "")
                reptime  = item.get("repairtime", "")
                date_str = offtime[:10] if offtime else ""
                count    = int(item.get("number") or 0)
                for area in item.get("areas", []):
                    records.append({
                        "date":          date_str,
                        "pref_name":     area.get("name", ""),
                        "count":         count,
                        "raw_reason":    raw,
                        "cause":         _normalize_reason(raw),
                        "company":       "関西電力送配電",
                        "start_time":    offtime,
                        "recovery_time": reptime,
                    })
    except Exception as exc:
        logger.warning("関西電力履歴取得失敗: %s", exc)
    return records


def fetch_shikoku_history() -> list[dict]:
    base    = "https://www.yonden.co.jp/nw/teiden-info/"
    referer = "https://www.yonden.co.jp/nw/teiden-info/history.html"
    records: list[dict] = []
    try:
        r = requests.get(base + "history.html", headers={**_HEADERS, "Referer": referer}, timeout=12)
        soup = BeautifulSoup(r.content, "html.parser")
        sel  = soup.find("select", id="date-select_select")
        file_nos: list[str] = []
        if sel:
            for opt in sel.find_all("option"):
                val = opt.get("value", "")
                if val and val != "0":
                    file_nos.append(("00" + str(val))[-2:])
        else:
            file_nos = [("00" + str(i))[-2:] for i in range(1, 32)]
        for file_no in file_nos:
            page_url = base + ("history.html" if file_no == "01" else f"history{file_no}.html")
            try:
                pr = requests.get(page_url, headers={**_HEADERS, "Referer": referer}, timeout=10)
                ps = BeautifulSoup(pr.content, "html.parser")
                lines = [ln.strip() for ln in ps.get_text("\n").split("\n") if ln.strip()]
                date_str = ""
                for ln in lines:
                    m = re.search(r"(\d{4}年\d+月\d+日)発生", ln)
                    if m:
                        date_str = m.group(1)
                        break
                tbl = ps.find("table")
                if not tbl:
                    continue
                for row in tbl.find_all("tr")[1:]:
                    cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                    if len(cells) < 3:
                        continue
                    start_t = cells[0]
                    end_t   = cells[1]
                    cnt_raw = cells[2]
                    nums    = re.findall(r"(\d[\d,]*)", cnt_raw)
                    count   = int(nums[0].replace(",", "")) if nums else 0
                    pref    = cells[3] if len(cells) > 3 else ""
                    matched_pref = ""
                    for p in _SHIKOKU_PREFS:
                        if p in (pref or start_t or "".join(cells)):
                            matched_pref = p
                            break
                    records.append({
                        "date":          date_str,
                        "pref_name":     matched_pref,
                        "count":         count,
                        "raw_reason":    "（起因情報なし）",
                        "cause":         "不明",
                        "company":       "四国電力送配電",
                        "start_time":    start_t,
                        "recovery_time": end_t,
                    })
            except Exception:
                pass
    except Exception as exc:
        logger.warning("四国電力履歴取得失敗: %s", exc)
    return records


def _parse_dt(date_str: str, time_str: str) -> Optional[datetime]:
    try:
        if not date_str or not time_str:
            return None
        if "-" in date_str:
            return datetime.strptime(time_str.strip(), "%Y-%m-%d %H:%M")
        year = int(re.search(r"(\d{4})年", date_str).group(1))
        cleaned = re.sub(r"(\d+)月(\d+)日\s+(\d+:\d+)", r"\1/\2 \3", time_str.strip())
        return datetime.strptime(f"{year}/{cleaned}", "%Y/%m/%d %H:%M")
    except Exception:
        return None


def fetch_tohoku_detail_df() -> pd.DataFrame:
    """東北電力ネットワーク専用の詳細停電履歴 DataFrame（過去31日）"""
    records = fetch_tohoku_history()
    rows: list[dict] = []
    for r in records:
        date_str = r.get("date", "")
        cause    = r["cause"]
        cat      = CAUSE_CATEGORY.get(cause, "不明・その他")
        try:
            dt = datetime.strptime(
                date_str.replace("年", "-").replace("月", "-").replace("日", ""),
                "%Y-%m-%d",
            )
            date_label = dt.strftime("%Y/%m/%d")
        except Exception:
            date_label = date_str
        s_dt = _parse_dt(date_str, r.get("start_time", ""))
        e_dt = _parse_dt(date_str, r.get("recovery_time", ""))
        duration_h = (
            round((e_dt - s_dt).total_seconds() / 3600, 2)
            if s_dt and e_dt and e_dt > s_dt else None
        )
        raw = r.get("raw_reason", "")
        rows.append({
            "date_label":     date_label,
            "pref_name":      r.get("pref_name", ""),
            "area_name":      r.get("area_name", ""),
            "count":          r["count"],
            "raw_reason":     raw,
            "cause":          cause,
            "cause_category": cat,
            "weather_flag":   r.get("weather_flag", _classify_weather(raw)),
            "start_time":     r.get("start_time", ""),
            "recovery_time":  r.get("recovery_time", ""),
            "duration_h":     duration_h,
        })
    if not rows:
        return pd.DataFrame(columns=[
            "date_label", "pref_name", "area_name", "count", "raw_reason",
            "cause", "cause_category", "weather_flag",
            "start_time", "recovery_time", "duration_h",
        ])
    return pd.DataFrame(rows)


def fetch_all_history_with_causes() -> pd.DataFrame:
    """全電力会社の実停電履歴（起因付き）を統一 DataFrame で返す"""
    raw: list[dict] = (
        fetch_hokkaido_history()
        + fetch_tohoku_history()
        + fetch_tepco_history()
        + fetch_kansai_history()
        + fetch_shikoku_history()
        + fetch_okinawa_history()
        + fetch_rikuden_history()
        + fetch_chugoku_history()
        + fetch_kyushu_history()
    )
    rows: list[dict] = []
    for r in raw:
        pref_name = r["pref_name"]
        master    = _PREF_MASTER.get(pref_name, {})
        cause     = r["cause"]
        cat       = CAUSE_CATEGORY.get(cause, "不明・その他")
        date_str  = r.get("date", "")
        try:
            if "-" in date_str:
                dt = datetime.strptime(date_str, "%Y-%m-%d")
            else:
                dt = datetime.strptime(
                    date_str.replace("年", "-").replace("月", "-").replace("日", ""),
                    "%Y-%m-%d",
                )
            month_label = dt.strftime("%Y/%m")
            date_label  = dt.strftime("%Y/%m/%d")
            month = dt.month
            year  = dt.year
        except Exception:
            month_label, date_label, month, year = "", "", 0, 0
        outage_hours = 0.0
        s_dt = _parse_dt(date_str, r.get("start_time", ""))
        e_dt = _parse_dt(date_str, r.get("recovery_time", ""))
        if s_dt and e_dt and e_dt > s_dt:
            outage_hours = round((e_dt - s_dt).total_seconds() / 3600, 2)
        raw_reason = r.get("raw_reason", "")
        rows.append({
            "date_label":         date_label,
            "month_label":        month_label,
            "year":               year,
            "month":              month,
            "prefecture":         pref_name,
            "code":               str(master.get("code", "")),
            "company":            r["company"],
            "region":             master.get("region", ""),
            "incidents":          1,
            "affected_customers": r["count"],
            "total_outage_hours": outage_hours,
            "cause":              cause,
            "cause_category":     cat,
            "raw_reason":         raw_reason,
            "weather_flag":       _classify_weather(raw_reason),
            "start_time":         r.get("start_time", ""),
            "data_source":        "実データ",
        })
    if not rows:
        return pd.DataFrame(columns=[
            "date_label", "month_label", "year", "month", "prefecture", "code",
            "company", "region", "incidents", "affected_customers",
            "total_outage_hours", "cause", "cause_category", "raw_reason",
            "weather_flag", "start_time", "data_source",
        ])
    return pd.DataFrame(rows)


# ── 全社まとめ取得 ─────────────────────────────────────────────

def fetch_all_realtime() -> pd.DataFrame:
    """アクセス可能な全電力会社から停電情報を取得し、47都道府県分の DataFrame を返す"""
    fetched: dict[str, dict] = {}
    fetch_time = datetime.now().strftime("%Y年%m月%d日 %H:%M")
    for fetch_fn, source_name, source_url in [
        (fetch_hokkaido, "北海道電力ネットワーク",
         "https://teiden-info.hepco.co.jp/"),
        (fetch_tohoku,   "東北電力ネットワーク",
         "https://nw.tohoku-epco.co.jp/teideninfo/"),
        (fetch_rikuden,  "北陸電力送配電",
         "https://www.rikuden.co.jp/nw/teiden/otj010.html"),
        (fetch_chubu,    "中部電力パワーグリッド",
         "https://teiden.powergrid.chuden.co.jp/p/index.html"),
        (fetch_tepco,    "東京電力パワーグリッド",
         "https://teideninfo.tepco.co.jp/"),
        (fetch_kansai,   "関西電力送配電",
         "https://www.kansai-td.co.jp/teiden-info/index.php"),
        (fetch_shikoku,  "四国電力送配電",
         "https://www.yonden.co.jp/nw/teiden-info/index.html"),
        (fetch_chugoku,  "中国電力ネットワーク",
         "https://www.teideninfo.energia.co.jp/"),
        (fetch_kyushu,   "九州電力送配電",
         "https://www.kyuden.co.jp/td_teiden/kyushu.html"),
        (fetch_okinawa,  "沖縄電力",
         "https://www.okidenmail.jp/bosai/info/index.html"),
    ]:
        pref_counts, ts = fetch_fn()
        for pref_name, count in pref_counts.items():
            fetched[pref_name] = {
                "count": count, "source": source_name,
                "source_url": source_url, "ts": ts or fetch_time,
            }
    rows = []
    for pref in PREFECTURES:
        name = pref["name"]
        info = fetched.get(name)
        if info is not None and info["count"] is not None:
            count, status = info["count"], "取得済み"
            source, source_url, ts = info["source"], info["source_url"], info["ts"]
        else:
            count, status = 0, "取得不可"
            source, source_url, ts = pref["company"], "", ""
        if status == "取得不可":
            level = scale_label = "データ未取得"
        elif count == 0:
            level = scale_label = "停電なし"
        elif count <= 1000:
            level = scale_label = "〜1,000軒"
        elif count <= 10000:
            level = scale_label = "1,001〜10,000軒"
        else:
            level = scale_label = "10,001軒以上"
        rows.append({
            "code":               str(pref["code"]),
            "prefecture":         name,
            "company":            pref["company"],
            "region":             pref["region"],
            "lat":                pref["lat"],
            "lon":                pref["lon"],
            "affected_customers": count,
            "outage_level":       level,
            "scale_label":        scale_label,
            "data_status":        status,
            "data_source":        source,
            "source_url":         source_url,
            "fetched_at":         ts if ts else fetch_time,
        })
    return pd.DataFrame(rows)
