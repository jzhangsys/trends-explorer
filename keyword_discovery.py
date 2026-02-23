"""
keyword_discovery.py
====================
動態高聲量關鍵字發現引擎

功能：
  1. 依應用場景（旅遊 / 健康 / 牙科 / 保健品）從種子關鍵字中選出聲量最高者
  2. 對高聲量關鍵字擴展出高度相關的搜尋詞
  3. 結果快取於 Supabase keyword_snapshots 表，TTL = 7 天

使用範例：
    from keyword_discovery import run_discovery
    result = run_discovery("旅遊", geo="TW", top_n=5)
    # result = {
    #   "scenario": "旅遊",
    #   "geo": "TW",
    #   "top_keywords": [{"keyword": "...", "avg_score": 82.3}, ...],
    #   "related_kws":  [{"keyword": "...", "source": "...", "type": "top|rising"}, ...],
    #   "cached_at": "2026-02-23T04:00:00+00:00",
    #   "from_cache": True/False,
    # }

Supabase 建表 SQL（第一次使用前請在 Supabase SQL Editor 執行）：
    create table if not exists keyword_snapshots (
      id            bigint generated always as identity primary key,
      scenario      text        not null,
      geo           text        not null default 'TW',
      top_keywords  jsonb       not null,
      related_kws   jsonb       not null,
      created_at    timestamptz not null default now()
    );
    create index if not exists idx_kw_snap_lookup
      on keyword_snapshots (scenario, geo, created_at desc);
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import pandas as pd
from pytrends.request import TrendReq
from pytrends.exceptions import TooManyRequestsError
from supabase import create_client, Client

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# 場景種子關鍵字（可自行編輯或擴充場景）
# ─────────────────────────────────────────────────────────────
SCENARIO_SEEDS: dict[str, list[str]] = {
    "旅遊": [
        "旅遊",
        "機票",
        "訂房",
        "背包客",
        "出國",
        "國內旅遊",
        "民宿",
        "旅行社",
        "自由行",
        "旅遊景點",
    ],
    "健康": [
        "健康",
        "養生",
        "運動",
        "睡眠",
        "心理健康",
        "飲食",
        "減重",
        "體重管理",
        "免疫力",
        "健檢",
    ],
    "牙科": [
        "牙科",
        "牙醫",
        "矯正",
        "植牙",
        "洗牙",
        "牙周病",
        "蛀牙",
        "假牙",
        "牙齒美白",
        "隱適美",
    ],
    "保健品": [
        "保健品",
        "維他命",
        "益生菌",
        "膠原蛋白",
        "魚油",
        "葉黃素",
        "鈣片",
        "保健食品",
        "營養補充",
        "抗氧化",
    ],
}

# 快取有效期（天）
CACHE_TTL_DAYS = 7

# pytrends 限速等待（秒）
RATE_LIMIT_SLEEP = 60
MAX_RETRIES = 3

# Google Trends 分析時間窗（發現高聲量關鍵字用）
DISCOVERY_TIMEFRAME = "today 1-m"   # 近 4 週

# 每次 build_payload 最多可放幾個關鍵字（Google Trends 上限 5）
CHUNK_SIZE = 5

# ─────────────────────────────────────────────────────────────
# 關鍵字 → 服務 / 商品映射（確定性高，人工策展）
# list 中第一個為主要服務，其後為延伸推薦
# ─────────────────────────────────────────────────────────────
KEYWORD_SERVICE_MAP: dict[str, list[str]] = {
    # ── 旅遊 ──────────────────────────────────────────────────
    "旅遊":     ["旅遊套裝行程", "旅遊保險", "旅遊信用卡"],
    "機票":     ["機票比價平台", "廉價航空票券", "商務艙升等"],
    "訂房":     ["訂房平台（Booking/Agoda）", "飯店比價", "早鳥優惠"],
    "背包客":   ["青年旅舍（Hostel）", "廉價機票", "旅遊行李箱"],
    "出國":     ["出國旅遊保險", "國際漫遊方案", "換匯服務"],
    "國內旅遊": ["國內民宿預訂", "高鐵/台鐵票券", "景點門票"],
    "民宿":     ["民宿預訂平台", "特色民宿體驗", "民宿禮券"],
    "旅行社":   ["套裝旅遊行程", "客製化旅遊規劃", "跟團旅遊"],
    "自由行":   ["自由行行程規劃", "景點票券", "租車服務"],
    "旅遊景點": ["景點門票預訂", "導覽解說服務", "周邊住宿"],
    "便宜機票": ["機票比價平台", "廉價航空訂票", "Last-minute 特惠"],
    "旅遊推薦": ["旅遊部落格廣告", "旅遊 App", "KOL 爆料合作"],
    "日本旅遊": ["日本旅遊套餐", "JR Pass 鐵路券", "日本 SIM 卡"],
    "韓國旅遊": ["韓國旅遊套餐", "K-ETA 電子旅行許可", "韓國 SIM 卡"],
    "歐洲旅遊": ["歐洲旅遊套餐", "申根保險", "歐洲火車通票"],
    "旅遊保險": ["旅遊平安險", "海外醫療險", "行李遺失理賠"],
    "租車":     ["租車平台", "國際駕照申請", "GPS 租賃"],

    # ── 健康 ──────────────────────────────────────────────────
    "健康":     ["健康檢查套組", "健康管理 App", "健康諮詢服務"],
    "養生":     ["養生食品", "中醫調理", "養生課程"],
    "運動":     ["健身房會員", "運動器材", "線上運動課程"],
    "睡眠":     ["助眠枕頭/床墊", "睡眠追蹤裝置", "助眠營養品"],
    "心理健康": ["心理諮商預約", "冥想 App", "壓力管理課程"],
    "飲食":     ["健康餐盒訂閱", "營養諮詢", "飲食記錄 App"],
    "減重":     ["減重計畫課程", "代餐/瘦身產品", "健身教練"],
    "體重管理": ["體重管理計畫", "代謝檢測", "低卡餐盒"],
    "免疫力":   ["免疫力保健品", "維他命 C/D", "中醫調補"],
    "健檢":     ["健康檢查套組", "健康檢查中心", "遠端健康監測"],
    "瑜珈":     ["瑜珈課程", "瑜珈墊/服裝", "線上瑜珈訂閱"],
    "健身":     ["健身房會籍", "個人教練", "蛋白質補充品"],
    "慢跑":     ["跑步鞋", "運動追蹤裝置", "馬拉松報名"],
    "排毒":     ["排毒飲品", "腸道保健品", "SPA 排毒療程"],

    # ── 牙科 ──────────────────────────────────────────────────
    "牙科":     ["牙科診所預約", "口腔健康保險", "電動牙刷"],
    "牙醫":     ["牙科診所推薦", "牙醫看診預約", "牙科健保方案"],
    "矯正":     ["牙齒矯正諮詢", "隱形矯正（隱適美）", "矯正費用估算"],
    "植牙":     ["植牙手術諮詢", "All-on-4 全口重建", "植牙分期付款"],
    "洗牙":     ["洗牙預約", "超音波潔牙", "居家潔牙組"],
    "牙周病":   ["牙周病治療", "牙周雷射療程", "牙周保養品"],
    "蛀牙":     ["蛀牙填補/根管治療", "兒童牙科", "防蛀牙膏"],
    "假牙":     ["陶瓷假牙", "活動假牙", "全瓷冠修復"],
    "牙齒美白": ["冷光美白療程", "居家美白貼片", "美白牙膏"],
    "隱適美":   ["隱適美矯正諮詢", "Invisalign 套組", "透明矯正器"],
    "牙套":     ["金屬矯正牙套", "陶瓷牙套", "夜間磨牙防護套"],
    "牙結石":   ["牙結石清除", "超音波洗牙", "抑菌漱口水"],

    # ── 保健品 ────────────────────────────────────────────────
    "保健品":   ["綜合保健品方案", "保健品訂閱盒", "保健品比價平台"],
    "維他命":   ["綜合維他命", "維他命 D3/K2", "兒童維他命軟糖"],
    "益生菌":   ["益生菌膠囊", "益生菌飲品", "腸道菌相檢測"],
    "膠原蛋白": ["膠原蛋白粉/飲", "口服美容保健品", "抗老化組合"],
    "魚油":     ["Omega-3 魚油", "深海魚油膠囊", "兒童魚油"],
    "葉黃素":   ["葉黃素護眼膠囊", "葉黃素飲品", "3C 護眼組合"],
    "鈣片":     ["鈣+D3 補充品", "兒童成長鈣", "老年骨骼保健"],
    "保健食品": ["功能性保健食品", "有機保健品", "台灣製保健品"],
    "營養補充": ["運動營養品", "術後營養補充", "全方位複合維生素"],
    "抗氧化":   ["抗氧化保健品（Q10）", "白藜蘆醇", "維他命 C 高劑量"],
    "蛋白質":   ["乳清蛋白", "植物性蛋白粉", "高蛋白飲食計畫"],
    "薑黃":     ["薑黃膠囊", "薑黃拿鐵", "消炎抗氧化組合"],
}

# 場景層級備用（關鍵字不在 KEYWORD_SERVICE_MAP 時使用）
SCENARIO_SERVICE_FALLBACK: dict[str, list[str]] = {
    "旅遊":  ["旅遊規劃服務", "住宿預訂", "旅遊保險"],
    "健康":  ["健康檢查", "保健品", "健身課程"],
    "牙科":  ["牙科診所諮詢", "口腔保健品", "矯正評估"],
    "保健品": ["保健品訂閱", "營養諮詢", "健康管理"],
}


def get_keyword_services(keyword: str, scenario: str = "") -> list[str]:
    """
    回傳該關鍵字對應的服務/商品推測。
    優先精確匹配，其次模糊匹配（包含關係），最後用場景備用。
    """
    # 1. 精確匹配
    if keyword in KEYWORD_SERVICE_MAP:
        return KEYWORD_SERVICE_MAP[keyword]

    # 2. 模糊匹配（關鍵字包含已知詞 or 已知詞包含關鍵字）
    for key, svcs in KEYWORD_SERVICE_MAP.items():
        if key in keyword or keyword in key:
            return svcs

    # 3. 場景備用
    if scenario and scenario in SCENARIO_SERVICE_FALLBACK:
        return SCENARIO_SERVICE_FALLBACK[scenario]

    return []



# ─────────────────────────────────────────────────────────────
# Supabase 連線（lazy singleton）
# ─────────────────────────────────────────────────────────────
SUPABASE_URL = "https://shiqrelmuvzwcxqndnyq.supabase.co"
SUPABASE_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
    ".eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNoaXFyZWxt"
    "dXZ6d2N4cW5kbnlxIiwicm9sZSI6InNlcnZpY2Vfcm9sZ"
    "SIsImlhdCI6MTczODMwNjk0MiwiZXhwIjoyMDUzODgyOTQyfQ"
    ".xiA87hhy0tOTytDmSmy_pRqeqVSLtEBqsrTxrvLy0ec"
)

_sb_client: Optional[Client] = None


def _get_supabase() -> Client:
    global _sb_client
    if _sb_client is None:
        _sb_client = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("Supabase 連線建立（keyword_discovery）")
    return _sb_client


# ─────────────────────────────────────────────────────────────
# pytrends 連線（lazy singleton）
# ─────────────────────────────────────────────────────────────
_pt_client: Optional[TrendReq] = None


def _get_pytrends() -> TrendReq:
    global _pt_client
    if _pt_client is None:
        _pt_client = TrendReq(
            hl="zh-TW",
            tz=-480,
            timeout=(10, 30),
            retries=2,
            backoff_factor=1.5,
        )
        logger.info("TrendReq 初始化完成（keyword_discovery）")
    return _pt_client


def _safe_call(fn, *args, **kwargs):
    """呼叫 pytrends API，遇到 429 限速則等待後重試。"""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except TooManyRequestsError:
            if attempt == MAX_RETRIES:
                raise
            logger.warning(
                "Google 限速 (429)，第 %d/%d 次重試，等待 %ds …",
                attempt, MAX_RETRIES, RATE_LIMIT_SLEEP,
            )
            time.sleep(RATE_LIMIT_SLEEP)


# ─────────────────────────────────────────────────────────────
# Step 1：從種子關鍵字中選出高聲量關鍵字
# ─────────────────────────────────────────────────────────────

def discover_top_keywords(
    scenario: str,
    geo: str = "TW",
    top_n: int = 5,
) -> list[dict]:
    """
    將場景種子關鍵字分批送入 Google Trends interest_over_time，
    計算近 4 週平均搜尋聲量，回傳聲量最高的 top_n 筆。

    Returns:
        [{"keyword": "旅遊", "avg_score": 82.3}, ...]  已按 avg_score 降序排列
    """
    seeds = SCENARIO_SEEDS.get(scenario)
    if not seeds:
        raise ValueError(f"未知場景：'{scenario}'，可用場景：{list(SCENARIO_SEEDS.keys())}")

    pt = _get_pytrends()
    scores: dict[str, float] = {}

    # 每批最多 CHUNK_SIZE 個（Google Trends 限制）
    chunks = [seeds[i: i + CHUNK_SIZE] for i in range(0, len(seeds), CHUNK_SIZE)]

    for idx, chunk in enumerate(chunks):
        logger.info("  [%s] 批次 %d/%d，關鍵字：%s", scenario, idx + 1, len(chunks), chunk)
        try:
            pt.build_payload(kw_list=chunk, timeframe=DISCOVERY_TIMEFRAME, geo=geo)
            df: pd.DataFrame = _safe_call(pt.interest_over_time)

            if df is not None and not df.empty:
                if "isPartial" in df.columns:
                    df = df.drop(columns=["isPartial"])
                for kw in chunk:
                    if kw in df.columns:
                        scores[kw] = round(float(df[kw].mean()), 2)
                    else:
                        scores[kw] = 0.0
            else:
                for kw in chunk:
                    scores[kw] = 0.0

        except Exception as exc:
            logger.warning("  批次 %s 失敗，略過：%s", chunk, exc)
            for kw in chunk:
                scores[kw] = 0.0

        # 批次之間稍作停頓
        if idx < len(chunks) - 1:
            time.sleep(3)

    # 排序並取 top_n
    sorted_kws = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_n]
    result = [{"keyword": kw, "avg_score": score} for kw, score in sorted_kws]
    logger.info("  [%s] Top %d 高聲量關鍵字：%s", scenario, top_n, result)
    return result


# ─────────────────────────────────────────────────────────────
# Step 2：擴展相關關鍵字
# ─────────────────────────────────────────────────────────────

def expand_related_keywords(
    top_keywords: list[dict],
    geo: str = "TW",
    max_per_kw: int = 10,
) -> list[dict]:
    """
    對每個高聲量關鍵字呼叫 related_queries，取 top + rising 各最多 max_per_kw 筆，
    去重後回傳。

    Returns:
        [{"keyword": "便宜機票", "source": "機票", "type": "top", "value": 100}, ...]
    """
    pt = _get_pytrends()
    seen: set[str] = set()
    related: list[dict] = []

    kw_list = [item["keyword"] for item in top_keywords]

    # related_queries 每次最多 5 個關鍵字，這裡已假設 top_n ≤ 5
    # 若 top_n > 5，分批處理
    chunks = [kw_list[i: i + CHUNK_SIZE] for i in range(0, len(kw_list), CHUNK_SIZE)]

    for idx, chunk in enumerate(chunks):
        logger.info("  [related] 批次 %d/%d，關鍵字：%s", idx + 1, len(chunks), chunk)
        try:
            pt.build_payload(kw_list=chunk, timeframe=DISCOVERY_TIMEFRAME, geo=geo)
            result: dict = _safe_call(pt.related_queries)

            for kw in chunk:
                kw_data = result.get(kw, {}) if result else {}

                for qtype in ("top", "rising"):
                    df = kw_data.get(qtype)
                    if df is None or df.empty:
                        continue
                    df = df.head(max_per_kw)
                    for _, row in df.iterrows():
                        q = str(row.get("query", "")).strip()
                        if q and q not in seen:
                            seen.add(q)
                            related.append({
                                "keyword": q,
                                "source": kw,
                                "type": qtype,
                                "value": int(row.get("value", 0)),
                            })

        except Exception as exc:
            logger.warning("  related_queries 批次 %s 失敗：%s", chunk, exc)

        if idx < len(chunks) - 1:
            time.sleep(3)

    logger.info("  相關關鍵字共 %d 筆（去重後）", len(related))
    return related


# ─────────────────────────────────────────────────────────────
# 快取存取（Supabase）
# ─────────────────────────────────────────────────────────────

def _load_cache(scenario: str, geo: str) -> Optional[dict]:
    """
    若 Supabase 內有該場景的新鮮快照（< CACHE_TTL_DAYS 天），回傳之；否則 None。
    """
    try:
        sb = _get_supabase()
        ttl_cutoff = (
            datetime.now(timezone.utc) - timedelta(days=CACHE_TTL_DAYS)
        ).isoformat()

        resp = (
            sb.table("keyword_snapshots")
            .select("id, top_keywords, related_kws, created_at")
            .eq("scenario", scenario)
            .eq("geo", geo)
            .gte("created_at", ttl_cutoff)
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )

        if resp.data:
            row = resp.data[0]
            logger.info(
                "快取命中：場景=%s, geo=%s, 建立於 %s",
                scenario, geo, row["created_at"],
            )
            return row
        return None

    except Exception as exc:
        logger.warning("讀取 Supabase 快取失敗（將重新抓取）：%s", exc)
        return None


def _save_cache(
    scenario: str,
    geo: str,
    top_keywords: list[dict],
    related_kws: list[dict],
) -> str:
    """
    將發現結果寫入 Supabase keyword_snapshots，回傳 created_at 時間字串。
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        sb = _get_supabase()
        sb.table("keyword_snapshots").insert({
            "scenario": scenario,
            "geo": geo,
            "top_keywords": top_keywords,
            "related_kws": related_kws,
            "created_at": now_iso,
        }).execute()
        logger.info("快取已儲存：場景=%s, geo=%s", scenario, geo)
    except Exception as exc:
        logger.warning("儲存 Supabase 快取失敗（不影響主功能）：%s", exc)
    return now_iso


# ─────────────────────────────────────────────────────────────
# 主要對外 API
# ─────────────────────────────────────────────────────────────

def run_discovery(
    scenario: str,
    geo: str = "TW",
    top_n: int = 5,
    force_refresh: bool = False,
) -> dict:
    """
    主要入口：發現特定場景的高聲量關鍵字及其相關關鍵字。

    Args:
        scenario:      場景名稱，須存在於 SCENARIO_SEEDS（旅遊 / 健康 / 牙科 / 保健品）
        geo:           Google Trends 地區代碼（預設 TW）
        top_n:         取幾個高聲量關鍵字（預設 5，最大建議 5 以符合 API 限制）
        force_refresh: True 時忽略快取，強制重新從 Google Trends 抓取

    Returns:
        {
          "scenario": str,
          "geo": str,
          "top_keywords":  [{"keyword": str, "avg_score": float}, ...],
          "related_kws":   [{"keyword": str, "source": str, "type": str, "value": int}, ...],
          "cached_at": str (ISO 8601),
          "from_cache": bool,
        }
    """
    if scenario not in SCENARIO_SEEDS:
        raise ValueError(
            f"未知場景：'{scenario}'，可用場景：{list(SCENARIO_SEEDS.keys())}"
        )

    # 1. 嘗試快取
    if not force_refresh:
        cached = _load_cache(scenario, geo)
        if cached:
            # 快取命中時即時補充 services（不存在快取中，確保 map 更新立即生效）
            top_kws = [
                {**kw, "services": get_keyword_services(kw["keyword"], scenario)}
                for kw in cached["top_keywords"]
            ]
            rel_kws = [
                {**kw, "services": get_keyword_services(kw["keyword"], scenario)}
                for kw in cached["related_kws"]
            ]
            return {
                "scenario": scenario,
                "geo": geo,
                "top_keywords": top_kws,
                "related_kws": rel_kws,
                "cached_at": cached["created_at"],
                "from_cache": True,
            }

    logger.info("開始發現場景 [%s]（geo=%s, top_n=%d）…", scenario, geo, top_n)

    # 2. 高聲量關鍵字發現
    top_keywords_raw = discover_top_keywords(scenario, geo=geo, top_n=top_n)

    time.sleep(3)   # 避免連續呼叫觸發限速

    # 3. 相關關鍵字擴展
    related_kws_raw = expand_related_keywords(top_keywords_raw, geo=geo)

    # 4. 寫入 Supabase 快取（不含 services，讓 map 更新可立即反映）
    cached_at = _save_cache(scenario, geo, top_keywords_raw, related_kws_raw)

    # 5. 即時注入 services 欄位
    top_keywords = [
        {**kw, "services": get_keyword_services(kw["keyword"], scenario)}
        for kw in top_keywords_raw
    ]
    related_kws = [
        {**kw, "services": get_keyword_services(kw["keyword"], scenario)}
        for kw in related_kws_raw
    ]

    return {
        "scenario": scenario,
        "geo": geo,
        "top_keywords": top_keywords,
        "related_kws": related_kws,
        "cached_at": cached_at,
        "from_cache": False,
    }


def list_scenarios() -> list[str]:
    """回傳所有可用場景名稱。"""
    return list(SCENARIO_SEEDS.keys())


# ─────────────────────────────────────────────────────────────
# CLI 快速測試
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import json
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    import sys
    scenario_arg = sys.argv[1] if len(sys.argv) > 1 else "旅遊"
    force_arg = "--force" in sys.argv

    print(f"\n{'='*55}")
    print(f"  Keyword Discovery — 場景：{scenario_arg}")
    print(f"{'='*55}\n")

    r = run_discovery(scenario_arg, geo="TW", top_n=5, force_refresh=force_arg)

    print(f"\n✅ 來源：{'快取' if r['from_cache'] else '即時抓取'}（{r['cached_at']}）")
    print("\n📊 高聲量關鍵字：")
    for i, kw in enumerate(r["top_keywords"], 1):
        print(f"  {i}. {kw['keyword']:15s}  avg_score={kw['avg_score']}")

    print(f"\n🔗 相關關鍵字（共 {len(r['related_kws'])} 筆）：")
    for kw in r["related_kws"][:15]:
        tag = "🔼" if kw["type"] == "rising" else "🔸"
        print(f"  {tag} {kw['keyword']:20s}  來源={kw['source']}  值={kw['value']}")
    if len(r["related_kws"]) > 15:
        print(f"  … 還有 {len(r['related_kws'])-15} 筆")
