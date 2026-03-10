"""
各電力ネットワーク会社ホームページからリアルタイム停電情報および
停電履歴（起因付き）を取得するモジュール。

リアルタイム取得可能:
  ・東北電力ネットワーク  ── JSON API    (青森/岩手/宮城/秋田/山形/福島/新潟)
  ・関西電力送配電        ── JSON API    (滋賀/京都/大阪/兵庫/奈良/和歌山/福井/岐阜/三重)
  ・四国電力送配電        ── HTML解析    (香川/愛媛/徳島/高知)

履歴・起因データ取得可能:
  ・東北電力ネットワーク  ── JSON API    (過去31日・起因付き)
  ・関西電力送配電        ── JSON API    (過去7日・起因付き)
  ・四国電力送配電        ── HTML解析    (過去31日・件数のみ、起因なし)

取得不可（JS動的レンダリング・認証必要等）:
  ・北海道電力ネットワーク / 東京電力パワーグリッド / 中部電力パワーグリッド
  ・北陸電力送配電 / 中国電力ネットワーク / 九州電力送配電 / 沖縄電力
"""

import re
import logging
from datetime import datetime
from typing import Optional

import requests
from bs4 import BeautifulSoup
import pandas as pd

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

# ── 停電起因マッピング（各社の生テキスト → 標準起因名）─────────
_REASON_MAP: dict[str, str] = {
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
    # 関西電力送配電
    "弊社設備への樹木や鳥獣等の接触": "樹木接触",
    "弊社設備の不具合":               "設備故障",
    "弊社設備に対する作業中の不具合": "設備故障",
    "風や雨の影響":                   "強風",
    "お客さま設備の不具合による影響": "その他",
    "調査の結果、原因不明":           "不明",
}

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
    # ── 天候 + 樹木・倒木（複合） ────────────────────────────
    "樹木接触・倒木の影響（氷雪）":  ["天候", "樹木・倒木"],
    "樹木接触・倒木の影響（風雨）":  ["天候", "樹木・倒木"],
    # ── 樹木・倒木のみ ────────────────────────────────────────
    "樹木接触・倒木の影響":          ["樹木・倒木"],  # 天候起因か不明
    "弊社設備への樹木や鳥獣等の接触": ["樹木・倒木"], # 関西電力
    # ── 設備 ──────────────────────────────────────────────────
    "弊社設備の故障":                ["設備"],
    "弊社設備の不具合":              ["設備"],        # 関西電力
    "弊社設備に対する作業中の不具合": ["設備"],       # 関西電力
    "お客さま設備故障の影響":        ["設備"],
    "お客さま設備の不具合による影響": ["設備"],       # 関西電力
    # ── 外的要因 ──────────────────────────────────────────────
    "車両衝突・接触等による影響":    ["外的要因"],
    "近隣火災による影響":            ["外的要因"],
    "第三者による過失等の影響":      ["外的要因"],
    "鳥獣・営巣等の影響":            ["外的要因"],
    "塩・ばい煙等の影響":            ["外的要因"],
    # ── 不明 ──────────────────────────────────────────────────
    "調査中":                        ["不明"],
    "調査の結果，特定できず":        ["不明"],
    "調査の結果、特定できず":        ["不明"],
    "調査の結果、原因不明":          ["不明"],        # 関西電力
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

def fetch_tohoku() -> tuple[dict[str, int], str]:
    """
    東北電力ネットワーク JSON API
    https://nw.tohoku-epco.co.jp/teideninfo/blackout/top.json
    戻り値: (dict[都道府県名 → 軒数], 更新日時文字列)
    """
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
        # 取得できなかった県は 0 で補完
        for pref in _TOHOKU_PREFS.values():
            result.setdefault(pref, 0)
        ts = data.get("time", "")
        return result, ts
    except Exception as exc:
        logger.warning("東北電力NW取得失敗: %s", exc)
        return {p: None for p in _TOHOKU_PREFS.values()}, ""


def fetch_kansai() -> tuple[dict[str, int], str]:
    """
    関西電力送配電 JSON API
    https://www.kansai-td.co.jp/interchange/teiden-info/ja/alert.json
    children: [] なら全管内 0 軒、それ以外は要素を再帰的に走査。
    戻り値: (dict[都道府県名 → 軒数], 更新日時文字列)
    """
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

        # children=[] （停電なし）のときは全管内を 0 で登録
        if not result:
            result = {p: 0 for p in _KANSAI_PREFS.values()}

        ts = data.get("datetime", "")
        return result, ts
    except Exception as exc:
        logger.warning("関西電力送配電取得失敗: %s", exc)
        return {p: None for p in _KANSAI_PREFS.values()}, ""


def fetch_shikoku() -> tuple[dict[str, int], str]:
    """
    四国電力送配電 HTMLスクレイピング
    https://www.yonden.co.jp/nw/teiden-info/index.html
    各県のブロックに「停電情報はありません」または軒数が記載される。
    戻り値: (dict[都道府県名 → 軒数], 更新日時文字列)
    """
    url = "https://www.yonden.co.jp/nw/teiden-info/index.html"
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=12)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        lines = [ln.strip() for ln in soup.get_text(separator="\n").split("\n") if ln.strip()]

        result: dict[str, int] = {}
        ts = ""

        # タイムスタンプ抽出（例: "2026年3月9日 22時15分 現在"）
        for ln in lines:
            if re.search(r"\d{4}年\d+月\d+日", ln) and "現在" in ln:
                ts = ln
                break

        # 各県の直後の行に停電情報が続く
        for i, line in enumerate(lines):
            for pref in _SHIKOKU_PREFS:
                if line == pref and i + 1 < len(lines):
                    nxt = lines[i + 1]
                    if "停電情報はありません" in nxt:
                        result[pref] = 0
                    else:
                        # "約XXX軒" or "XXX軒" を取得
                        nums = re.findall(r"(\d[\d,]*)\s*軒", nxt)
                        result[pref] = int(nums[0].replace(",", "")) if nums else 0

        for pref in _SHIKOKU_PREFS:
            result.setdefault(pref, 0)

        return result, ts
    except Exception as exc:
        logger.warning("四国電力送配電取得失敗: %s", exc)
        return {p: None for p in _SHIKOKU_PREFS}, ""


# ── 履歴・起因取得関数 ────────────────────────────────────────

def fetch_tohoku_history() -> list[dict]:
    """
    東北電力ネットワーク 過去31日間の停電履歴（起因付き）
    https://nw.tohoku-epco.co.jp/teideninfo/rireki.html?pref=&time=05
    戻り値: list[dict] (date, pref_name, count, raw_reason, cause, start_time, recovery_time)
    """
    base    = "https://nw.tohoku-epco.co.jp/teideninfo/blackout/"
    referer = "https://nw.tohoku-epco.co.jp/teideninfo/rireki.html"
    hdrs    = {**_HEADERS, "Referer": referer}
    records: list[dict] = []

    try:
        top  = requests.get(base + "rirekiinfo_top.json", headers=hdrs, timeout=12).json()
        for date_item in top.get("dates", []):
            file_no  = date_item["file_no"]
            date_str = date_item["date"]   # "2026年3月6日"
            try:
                data = requests.get(
                    base + f"rirekiinfo{file_no}.json",
                    headers=hdrs, timeout=10,
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
    """
    関西電力送配電 過去7日間の停電履歴（起因付き）
    https://www.kansai-td.co.jp/interchange/teiden-info/ja/history.json
    戻り値: list[dict] (date, pref_name, count, raw_reason, cause, start_time, recovery_time)
    """
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
    """
    四国電力送配電 過去31日間の停電履歴（件数のみ、起因情報なし）
    https://www.yonden.co.jp/nw/teiden-info/history.html
    戻り値: list[dict] (date, pref_name, count, raw_reason, cause, start_time, recovery_time)
    """
    base    = "https://www.yonden.co.jp/nw/teiden-info/"
    referer = "https://www.yonden.co.jp/nw/teiden-info/history.html"
    records: list[dict] = []

    try:
        # まず日付一覧を history.html から取得
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

                # ページタイトルから日付取得 "過去の停電一覧(2026年3月10日発生)"
                date_str = ""
                for ln in lines:
                    m = re.search(r"(\d{4}年\d+月\d+日)発生", ln)
                    if m:
                        date_str = m.group(1)
                        break

                # テーブル行を解析（発生日時・復旧日時・停電戸数 の順）
                tbl = ps.find("table")
                if not tbl:
                    continue
                rows_el = tbl.find_all("tr")
                for row in rows_el[1:]:  # ヘッダースキップ
                    cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                    if len(cells) < 3:
                        continue
                    start_t = cells[0] if len(cells) > 0 else ""
                    end_t   = cells[1] if len(cells) > 1 else ""
                    cnt_raw = cells[2] if len(cells) > 2 else "0"
                    nums    = re.findall(r"(\d[\d,]*)", cnt_raw)
                    count   = int(nums[0].replace(",", "")) if nums else 0
                    pref    = cells[3] if len(cells) > 3 else ""

                    # 県名が含まれるか確認
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
    """日付文字列と時刻文字列を組み合わせて datetime を返す。失敗時は None。"""
    try:
        if not date_str or not time_str:
            return None
        if "-" in date_str:
            # "2026-03-06" + "2026-03-06 07:45"
            return datetime.strptime(time_str.strip(), "%Y-%m-%d %H:%M")
        # "2026年3月6日" + "3月6日 07:45"
        year = int(re.search(r"(\d{4})年", date_str).group(1))
        cleaned = re.sub(r"(\d+)月(\d+)日\s+(\d+:\d+)", r"\1/\2 \3", time_str.strip())
        return datetime.strptime(f"{year}/{cleaned}", "%Y/%m/%d %H:%M")
    except Exception:
        return None


def fetch_tohoku_detail_df() -> pd.DataFrame:
    """
    東北電力ネットワーク専用の詳細停電履歴 DataFrame（過去31日）。
    地域名・発生時刻・復旧時刻・停電時間を含む。

    カラム:
        date_label, pref_name, area_name, count, raw_reason, cause,
        cause_category, start_time, recovery_time, duration_h
    """
    records = fetch_tohoku_history()
    rows: list[dict] = []

    for r in records:
        date_str = r.get("date", "")
        cause    = r["cause"]
        cat      = CAUSE_CATEGORY.get(cause, "不明・その他")

        # date_label
        try:
            dt = datetime.strptime(
                date_str.replace("年", "-").replace("月", "-").replace("日", ""),
                "%Y-%m-%d",
            )
            date_label = dt.strftime("%Y/%m/%d")
            year = dt.year
        except Exception:
            date_label, year = date_str, datetime.now().year

        # 停電時間計算（東北の時刻フォーマット "3月6日 07:45"）
        s_dt = _parse_dt(date_str, r.get("start_time", ""))
        e_dt = _parse_dt(date_str, r.get("recovery_time", ""))
        if s_dt and e_dt and e_dt > s_dt:
            duration_h = round((e_dt - s_dt).total_seconds() / 3600, 2)
        else:
            duration_h = None

        raw = r.get("raw_reason", "")
        rows.append({
            "date_label":    date_label,
            "pref_name":     r.get("pref_name", ""),
            "area_name":     r.get("area_name", ""),
            "count":         r["count"],
            "raw_reason":    raw,
            "cause":         cause,
            "cause_category": cat,
            "weather_flag":  r.get("weather_flag", _classify_weather(raw)),
            "start_time":    r.get("start_time", ""),
            "recovery_time": r.get("recovery_time", ""),
            "duration_h":    duration_h,
        })

    if not rows:
        return pd.DataFrame(columns=[
            "date_label", "pref_name", "area_name", "count", "raw_reason",
            "cause", "cause_category", "weather_flag",
            "start_time", "recovery_time", "duration_h",
        ])
    return pd.DataFrame(rows)


def fetch_all_history_with_causes() -> pd.DataFrame:
    """
    東北電力ネットワーク（31日）・関西電力送配電（7日）・四国電力送配電（31日）の
    実停電履歴（起因付き）を取得して統一 DataFrame を返す。

    カラム:
        month_label, year, month, prefecture, code, company, region,
        incidents (= 1件ずつ), affected_customers, total_outage_hours,
        cause, cause_category, raw_reason, data_source
    """
    raw: list[dict] = (
        fetch_tohoku_history()
        + fetch_kansai_history()
        + fetch_shikoku_history()
    )

    rows: list[dict] = []
    for r in raw:
        pref_name = r["pref_name"]
        master    = _PREF_MASTER.get(pref_name, {})
        cause     = r["cause"]
        cat       = CAUSE_CATEGORY.get(cause, "不明・その他")

        date_str = r.get("date", "")
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

        # 停電時間を計算
        outage_hours = 0.0
        s_dt = _parse_dt(date_str, r.get("start_time", ""))
        e_dt = _parse_dt(date_str, r.get("recovery_time", ""))
        if s_dt and e_dt and e_dt > s_dt:
            outage_hours = round((e_dt - s_dt).total_seconds() / 3600, 2)

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
            "raw_reason":         r.get("raw_reason", ""),
            "data_source":        "実データ",
        })

    if not rows:
        return pd.DataFrame(columns=[
            "date_label", "month_label", "year", "month", "prefecture", "code",
            "company", "region", "incidents", "affected_customers",
            "total_outage_hours", "cause", "cause_category", "raw_reason", "data_source",
        ])
    return pd.DataFrame(rows)


# ── 全社まとめ取得 ─────────────────────────────────────────────

def fetch_all_realtime() -> pd.DataFrame:
    """
    アクセス可能な全電力会社から停電情報を取得し、47都道府県分の
    DataFrame を返す。取得不可の都道府県は data_status="取得不可" で補完。

    カラム:
        code, prefecture, company, region, lat, lon,
        affected_customers (int|NaN),
        outage_level  (str: 停電なし/〜1,000軒/1,001〜10,000軒/10,001軒以上/データ未取得),
        data_status   (str: 取得済み/取得不可),
        data_source   (str: 電力会社名),
        source_url    (str),
        fetched_at    (str),
        scale_label   (str: Yahoo準拠の3区分),
    """
    # ── 各社取得 ──────────────────────────────────────────────
    fetched: dict[str, dict] = {}
    fetch_time = datetime.now().strftime("%Y年%m月%d日 %H:%M")

    for fetch_fn, source_name, source_url in [
        (fetch_tohoku,  "東北電力ネットワーク",
         "https://nw.tohoku-epco.co.jp/teideninfo/"),
        (fetch_kansai,  "関西電力送配電",
         "https://www.kansai-td.co.jp/teiden-info/index.php"),
        (fetch_shikoku, "四国電力送配電",
         "https://www.yonden.co.jp/nw/teiden-info/index.html"),
    ]:
        pref_counts, ts = fetch_fn()
        for pref_name, count in pref_counts.items():
            fetched[pref_name] = {
                "count":      count,
                "source":     source_name,
                "source_url": source_url,
                "ts":         ts or fetch_time,
            }

    # ── 47都道府県 DataFrame 構築 ──────────────────────────────
    rows = []
    for pref in PREFECTURES:
        name = pref["name"]
        info = fetched.get(name)

        if info is not None and info["count"] is not None:
            count       = info["count"]
            status      = "取得済み"
            source      = info["source"]
            source_url  = info["source_url"]
            ts          = info["ts"]
        else:
            count       = 0          # 地図描画のため 0 として扱う
            status      = "取得不可"
            source      = pref["company"]
            source_url  = ""
            ts          = ""

        # 停電規模ラベル（Yahoo準拠）
        if status == "取得不可":
            level       = "データ未取得"
            scale_label = "データ未取得"
        elif count == 0:
            level       = "停電なし"
            scale_label = "停電なし"
        elif count <= 1000:
            level       = "〜1,000軒"
            scale_label = "〜1,000軒"
        elif count <= 10000:
            level       = "1,001〜10,000軒"
            scale_label = "1,001〜10,000軒"
        else:
            level       = "10,001軒以上"
            scale_label = "10,001軒以上"

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
