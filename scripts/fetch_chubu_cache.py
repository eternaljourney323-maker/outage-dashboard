"""
GitHub Actions から実行: 中部電力PGの停電データを取得して cache/chubu.json に保存する。
取得失敗時はファイルを書き込まず exit(1) で終了する（既存キャッシュを保護）。
"""
import json
import re
import time
from datetime import datetime, timezone

try:
    from curl_cffi import requests as cffi_requests
    session = cffi_requests.Session(impersonate="chrome124")
except ImportError:
    import requests as _req
    session = _req.Session()

BASE_URL = "https://teiden.powergrid.chuden.co.jp/p"
PREFS = ["愛知県", "三重県", "岐阜県", "静岡県", "長野県"]

_NAV_HDRS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en;q=0.9",
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "Upgrade-Insecure-Requests": "1",
}
_XHR_HDRS = {
    **_NAV_HDRS,
    "Accept": "application/xml, text/xml, */*; q=0.01",
    "Referer": f"{BASE_URL}/index.html",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "X-Requested-With": "XMLHttpRequest",
}
for k in ("Upgrade-Insecure-Requests", "sec-fetch-user"):
    _XHR_HDRS.pop(k, None)


def fetch():
    """取得成功時は dict を返す。失敗時は None を返す。"""
    top_url = f"{BASE_URL}/index.html"
    try:
        session.get(top_url, headers=_NAV_HDRS, timeout=15)
    except Exception as e:
        print(f"top page error: {e}")

    ts_ms = int(time.time() * 1000)
    index_url = f"{BASE_URL}/resource/disclose/xml/index.xml?{ts_ms}"
    result = {p: 0 for p in PREFS}
    ts = ""

    try:
        from bs4 import BeautifulSoup
        r = session.get(index_url, headers=_XHR_HDRS, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "xml")
        areas = soup.find_all("area")
        if not areas:
            # teiden_info root exists but empty = zero outages (valid response)
            if soup.find("teiden_info") is None:
                raise ValueError(f"No area tags and no teiden_info root. body={r.text[:300]!r}")
        for area in areas:
            addr = area.find("address")
            kosu = area.find("genzai_teiden_kosu")
            if addr and kosu:
                pref = addr.text.strip()
                if pref in result:
                    nums = re.findall(r"[\d,]+", kosu.text)
                    if nums:
                        result[pref] = int(nums[0].replace(",", ""))

        area_url = f"{BASE_URL}/resource/xml/teiden_area.xml?{ts_ms}"
        ar = session.get(area_url, headers=_XHR_HDRS, timeout=20)
        ar.encoding = "utf-8"
        ar_soup = BeautifulSoup(ar.text, "xml")
        dt_node = ar_soup.find("data_make_d")
        if dt_node and dt_node.text:
            raw = dt_node.text.strip()
            try:
                ts = datetime.strptime(raw, "%Y/%m/%d %H:%M").strftime("%Y年%m月%d日 %H:%M")
            except Exception:
                ts = raw

        return {
            "result": result,
            "ts": ts,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        print(f"FETCH FAILED: {e}")
        return None


if __name__ == "__main__":
    import pathlib
    data = fetch()
    if data is None:
        print("FETCH FAILED - cache not updated")
        exit(1)
    print(json.dumps(data, ensure_ascii=False, indent=2))
    out = pathlib.Path(__file__).parent.parent / "cache" / "chubu.json"
    out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Saved to {out}")
    print("SUCCESS")
