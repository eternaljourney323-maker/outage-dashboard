import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

PREFECTURES = [
    {"code": 1,  "name": "北海道",   "name_en": "Hokkaido",   "company": "北海道電力", "region": "北海道", "lat": 43.46, "lon": 142.78},
    {"code": 2,  "name": "青森県",   "name_en": "Aomori",     "company": "東北電力",   "region": "東北",   "lat": 40.74, "lon": 140.74},
    {"code": 3,  "name": "岩手県",   "name_en": "Iwate",      "company": "東北電力",   "region": "東北",   "lat": 39.71, "lon": 141.13},
    {"code": 4,  "name": "宮城県",   "name_en": "Miyagi",     "company": "東北電力",   "region": "東北",   "lat": 38.27, "lon": 140.87},
    {"code": 5,  "name": "秋田県",   "name_en": "Akita",      "company": "東北電力",   "region": "東北",   "lat": 39.72, "lon": 140.10},
    {"code": 6,  "name": "山形県",   "name_en": "Yamagata",   "company": "東北電力",   "region": "東北",   "lat": 38.24, "lon": 140.36},
    {"code": 7,  "name": "福島県",   "name_en": "Fukushima",  "company": "東北電力",   "region": "東北",   "lat": 37.38, "lon": 140.19},
    {"code": 8,  "name": "茨城県",   "name_en": "Ibaraki",    "company": "東京電力",   "region": "関東",   "lat": 36.34, "lon": 140.44},
    {"code": 9,  "name": "栃木県",   "name_en": "Tochigi",    "company": "東京電力",   "region": "関東",   "lat": 36.57, "lon": 139.88},
    {"code": 10, "name": "群馬県",   "name_en": "Gunma",      "company": "東京電力",   "region": "関東",   "lat": 36.39, "lon": 139.07},
    {"code": 11, "name": "埼玉県",   "name_en": "Saitama",    "company": "東京電力",   "region": "関東",   "lat": 35.86, "lon": 139.65},
    {"code": 12, "name": "千葉県",   "name_en": "Chiba",      "company": "東京電力",   "region": "関東",   "lat": 35.61, "lon": 140.12},
    {"code": 13, "name": "東京都",   "name_en": "Tokyo",      "company": "東京電力",   "region": "関東",   "lat": 35.69, "lon": 139.69},
    {"code": 14, "name": "神奈川県", "name_en": "Kanagawa",   "company": "東京電力",   "region": "関東",   "lat": 35.45, "lon": 139.64},
    {"code": 15, "name": "新潟県",   "name_en": "Niigata",    "company": "東北電力",   "region": "中部",   "lat": 37.36, "lon": 138.95},
    {"code": 16, "name": "富山県",   "name_en": "Toyama",     "company": "北陸電力",   "region": "中部",   "lat": 36.70, "lon": 137.21},
    {"code": 17, "name": "石川県",   "name_en": "Ishikawa",   "company": "北陸電力",   "region": "中部",   "lat": 36.59, "lon": 136.63},
    {"code": 18, "name": "福井県",   "name_en": "Fukui",      "company": "北陸電力",   "region": "中部",   "lat": 35.95, "lon": 136.18},
    {"code": 19, "name": "山梨県",   "name_en": "Yamanashi",  "company": "東京電力",   "region": "中部",   "lat": 35.66, "lon": 138.57},
    {"code": 20, "name": "長野県",   "name_en": "Nagano",     "company": "中部電力",   "region": "中部",   "lat": 36.65, "lon": 138.19},
    {"code": 21, "name": "岐阜県",   "name_en": "Gifu",       "company": "中部電力",   "region": "中部",   "lat": 35.39, "lon": 136.72},
    {"code": 22, "name": "静岡県",   "name_en": "Shizuoka",   "company": "中部電力",   "region": "中部",   "lat": 34.98, "lon": 138.38},
    {"code": 23, "name": "愛知県",   "name_en": "Aichi",      "company": "中部電力",   "region": "中部",   "lat": 35.18, "lon": 137.10},
    {"code": 24, "name": "三重県",   "name_en": "Mie",        "company": "中部電力",   "region": "近畿",   "lat": 34.73, "lon": 136.51},
    {"code": 25, "name": "滋賀県",   "name_en": "Shiga",      "company": "関西電力",   "region": "近畿",   "lat": 35.00, "lon": 135.87},
    {"code": 26, "name": "京都府",   "name_en": "Kyoto",      "company": "関西電力",   "region": "近畿",   "lat": 35.02, "lon": 135.76},
    {"code": 27, "name": "大阪府",   "name_en": "Osaka",      "company": "関西電力",   "region": "近畿",   "lat": 34.69, "lon": 135.50},
    {"code": 28, "name": "兵庫県",   "name_en": "Hyogo",      "company": "関西電力",   "region": "近畿",   "lat": 34.69, "lon": 134.90},
    {"code": 29, "name": "奈良県",   "name_en": "Nara",       "company": "関西電力",   "region": "近畿",   "lat": 34.68, "lon": 135.83},
    {"code": 30, "name": "和歌山県", "name_en": "Wakayama",   "company": "関西電力",   "region": "近畿",   "lat": 33.94, "lon": 135.17},
    {"code": 31, "name": "鳥取県",   "name_en": "Tottori",    "company": "中国電力",   "region": "中国",   "lat": 35.50, "lon": 133.82},
    {"code": 32, "name": "島根県",   "name_en": "Shimane",    "company": "中国電力",   "region": "中国",   "lat": 35.47, "lon": 133.06},
    {"code": 33, "name": "岡山県",   "name_en": "Okayama",    "company": "中国電力",   "region": "中国",   "lat": 34.66, "lon": 133.93},
    {"code": 34, "name": "広島県",   "name_en": "Hiroshima",  "company": "中国電力",   "region": "中国",   "lat": 34.40, "lon": 132.46},
    {"code": 35, "name": "山口県",   "name_en": "Yamaguchi",  "company": "中国電力",   "region": "中国",   "lat": 34.17, "lon": 131.54},
    {"code": 36, "name": "徳島県",   "name_en": "Tokushima",  "company": "四国電力",   "region": "四国",   "lat": 33.83, "lon": 134.23},
    {"code": 37, "name": "香川県",   "name_en": "Kagawa",     "company": "四国電力",   "region": "四国",   "lat": 34.34, "lon": 134.04},
    {"code": 38, "name": "愛媛県",   "name_en": "Ehime",      "company": "四国電力",   "region": "四国",   "lat": 33.84, "lon": 132.77},
    {"code": 39, "name": "高知県",   "name_en": "Kochi",      "company": "四国電力",   "region": "四国",   "lat": 33.56, "lon": 133.53},
    {"code": 40, "name": "福岡県",   "name_en": "Fukuoka",    "company": "九州電力",   "region": "九州",   "lat": 33.60, "lon": 130.72},
    {"code": 41, "name": "佐賀県",   "name_en": "Saga",       "company": "九州電力",   "region": "九州",   "lat": 33.25, "lon": 130.30},
    {"code": 42, "name": "長崎県",   "name_en": "Nagasaki",   "company": "九州電力",   "region": "九州",   "lat": 32.92, "lon": 129.87},
    {"code": 43, "name": "熊本県",   "name_en": "Kumamoto",   "company": "九州電力",   "region": "九州",   "lat": 32.85, "lon": 130.75},
    {"code": 44, "name": "大分県",   "name_en": "Oita",       "company": "九州電力",   "region": "九州",   "lat": 33.24, "lon": 131.61},
    {"code": 45, "name": "宮崎県",   "name_en": "Miyazaki",   "company": "九州電力",   "region": "九州",   "lat": 31.91, "lon": 131.42},
    {"code": 46, "name": "鹿児島県", "name_en": "Kagoshima",  "company": "九州電力",   "region": "九州",   "lat": 31.59, "lon": 130.55},
    {"code": 47, "name": "沖縄県",   "name_en": "Okinawa",    "company": "沖縄電力",   "region": "沖縄",   "lat": 26.21, "lon": 127.68},
]

# 事故起因（電力会社レポート準拠の分類体系）
CAUSES = [
    "台風（暴風雨）",
    "落雷",
    "強風",
    "大雪・着雪",
    "地震",
    "塩害",
    "設備故障",
    "機器老朽化",
    "交通事故（電柱衝突）",
    "樹木接触",
    "火災",
    "工事（計画停電）",
    "不明",
    "その他",
]

# 事故起因カテゴリー（上位分類）
CAUSE_CATEGORY: dict[str, str] = {
    "台風（暴風雨）":       "自然災害",
    "落雷":                 "自然災害",
    "強風":                 "自然災害",
    "大雪・着雪":           "自然災害",
    "地震":                 "自然災害",
    "塩害":                 "自然災害",
    "設備故障":             "設備・機器",
    "機器老朽化":           "設備・機器",
    "交通事故（電柱衝突）": "外的要因",
    "樹木接触":             "外的要因",
    "火災":                 "外的要因",
    "工事（計画停電）":     "計画停電",
    "不明":                 "不明・その他",
    "その他":               "不明・その他",
}

CATEGORY_COLOR: dict[str, str] = {
    "自然災害":     "#ef4444",
    "設備・機器":   "#f97316",
    "外的要因":     "#a855f7",
    "計画停電":     "#3b82f6",
    "不明・その他": "#9ca3af",
}


def _cause_pool_for(month: int, region: str) -> list[str]:
    """月・地域に応じた事故起因の重み付きプール"""
    is_typhoon  = month in [8, 9, 10]
    is_winter   = month in [12, 1, 2, 3]
    is_summer   = month in [6, 7, 8]
    is_typhoon_region = region in ["九州", "沖縄", "四国", "近畿", "中国"]
    is_snow_region    = region in ["北海道", "東北", "中部", "北陸"]

    pool: list[str] = []
    if is_typhoon and is_typhoon_region:
        pool += ["台風（暴風雨）"] * 6 + ["強風"] * 3 + ["落雷"] * 1
    elif is_winter and is_snow_region:
        pool += ["大雪・着雪"] * 6 + ["強風"] * 2 + ["機器老朽化"] * 2
    elif is_summer:
        pool += ["落雷"] * 4 + ["台風（暴風雨）"] * 1 + ["強風"] * 1
    else:
        pool += ["強風"] * 1 + ["落雷"] * 1

    # 通年起因は常に追加
    pool += (
        ["設備故障"] * 3
        + ["機器老朽化"] * 2
        + ["交通事故（電柱衝突）"] * 2
        + ["樹木接触"] * 2
        + ["工事（計画停電）"] * 2
        + ["塩害"] * (1 if region in ["沖縄", "九州", "四国"] else 0)
        + ["火災"] * 1
        + ["不明"] * 1
        + ["その他"] * 1
    )
    return pool


def get_realtime_outages() -> pd.DataFrame:
    """現在の停電情報（1分ごとに変化するシミュレーション）"""
    seed = int(datetime.now().timestamp()) // 60
    rng = np.random.default_rng(seed)
    local_random = random.Random(seed)

    month = datetime.now().month
    n_affected = int(rng.integers(5, 16))
    affected_indices = local_random.sample(range(len(PREFECTURES)), n_affected)
    affected_set = set(affected_indices)

    rows = []
    for i, pref in enumerate(PREFECTURES):
        if i in affected_set:
            customers  = int(rng.exponential(4000)) + 50
            incidents  = int(rng.integers(1, 10))
            minutes_ago = int(rng.integers(5, 300))
            start_time  = datetime.now() - timedelta(minutes=minutes_ago)
            cause  = local_random.choice(_cause_pool_for(month, pref["region"]))
            category = CAUSE_CATEGORY[cause]
            scale_label = (
                "10,001軒以上" if customers > 10000
                else "1,001〜10,000軒" if customers > 1000
                else "〜1,000軒"
            )
        else:
            customers = 0; incidents = 0
            start_time = None; cause = "—"; category = "—"
            scale_label = "停電なし"

        rows.append({
            "code":               str(pref["code"]),
            "prefecture":         pref["name"],
            "company":            pref["company"],
            "region":             pref["region"],
            "lat":                pref["lat"],
            "lon":                pref["lon"],
            "incidents":          incidents,
            "affected_customers": customers,
            "scale_label":        scale_label,
            "cause":              cause,
            "cause_category":     category,
            "elapsed_min":        int((datetime.now() - start_time).total_seconds() // 60) if start_time else 0,
            "start_time":         start_time.strftime("%H:%M") if start_time else "—",
        })

    return pd.DataFrame(rows)


def get_historical_data() -> pd.DataFrame:
    """過去1年間の月次停電実績（シード固定・再現性あり）"""
    rng = np.random.default_rng(42)
    local_random = random.Random(42)

    now = datetime.now()
    records = []

    for months_back in range(12, 0, -1):
        target = now - timedelta(days=30 * months_back)
        year  = target.year
        month = target.month
        month_label = f"{year}/{month:02d}"

        is_typhoon = month in [8, 9, 10]
        is_winter  = month in [12, 1, 2]

        for pref in PREFECTURES:
            seasonal = 1.6 if (month in [7, 8, 9] or is_winter) else 1.0
            typhoon_boost = (
                2.5 if (is_typhoon and pref["region"] in ["九州", "沖縄", "四国"]) else 1.0
            )
            snow_boost = (
                1.8 if (is_winter and pref["region"] in ["北海道", "東北", "中部"]) else 1.0
            )
            factor = seasonal * max(typhoon_boost, snow_boost)

            incidents = int(rng.poisson(2.5 * factor))
            if incidents == 0:
                continue

            customers = int(rng.exponential(400 * factor)) + 10
            hours     = round(float(rng.uniform(0.5, 6.0)) * incidents, 1)
            cause     = local_random.choice(_cause_pool_for(month, pref["region"]))
            category  = CAUSE_CATEGORY[cause]

            records.append({
                "month_label":        month_label,
                "year":               year,
                "month":              month,
                "prefecture":         pref["name"],
                "code":               str(pref["code"]),
                "company":            pref["company"],
                "region":             pref["region"],
                "incidents":          incidents,
                "affected_customers": customers,
                "total_outage_hours": hours,
                "cause":              cause,
                "cause_category":     category,
            })

    return pd.DataFrame(records)
