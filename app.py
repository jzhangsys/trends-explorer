"""
trend_app/app.py
Google Trends Explorer — Flask Backend + Supabase 歷史記錄

啟動方式：
    cd examples/trend_app
    python3 app.py
"""

import time
import logging
import os
from datetime import datetime, timezone

from flask import Flask, jsonify, request, render_template
from pytrends.request import TrendReq
from pytrends.exceptions import TooManyRequestsError
from supabase import create_client, Client
from keyword_discovery import run_discovery, list_scenarios, SCENARIO_SEEDS
from hf_services import (
    suggest_services_ai,
    classify_keyword_scenario,
    semantic_keyword_search,
    summarize_trends,
    score_commercial_intent,
    analyze_platform_attribution,
)
from news_service import fetch_keyword_news

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__, template_folder="templates", static_folder="static")

# ── Supabase 設定 ─────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://shiqrelmuvzwcxqndnyq.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNoaXFyZWxtdXZ6d2N4cW5kbnlxIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTczODMwNjk0MiwiZXhwIjoyMDUzODgyOTQyfQ.xiA87hhy0tOTytDmSmy_pRqeqVSLtEBqsrTxrvLy0ec")

_sb: Client = None

def get_supabase() -> Client:
    global _sb
    if _sb is None:
        _sb = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("Supabase 連線建立完成")
    return _sb


def save_query_history(keywords: list, geo: str, timeframe: str, result_summary: dict):
    """將查詢紀錄寫入 Supabase 的 trend_query_history 表"""
    try:
        sb = get_supabase()
        sb.table("trend_query_history").insert({
            "keywords": keywords,
            "geo": geo,
            "timeframe": timeframe,
            "result_summary": result_summary,
            "queried_at": datetime.now(timezone.utc).isoformat(),
        }).execute()
        logger.info("歷史記錄已儲存：%s", keywords)
    except Exception as e:
        logger.warning("儲存歷史記錄失敗（不影響主功能）：%s", e)


# ── pytrends lazy init ────────────────────────────────────────
_pt = None

def get_pytrends() -> TrendReq:
    global _pt
    if _pt is None:
        logger.info("初始化 TrendReq（連接 Google 取 cookie）…")
        _pt = TrendReq(hl="zh-TW", tz=-480, timeout=(5, 15))
        logger.info("TrendReq 初始化完成")
    return _pt


def safe_call(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except TooManyRequestsError:
        logger.warning("Google 限速中，等待 60s 重試…")
        time.sleep(60)
        return fn(*args, **kwargs)


# ── 前端頁面 ──────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")


# ── API: Interest Over Time ───────────────────────────────────
@app.route("/api/interest-over-time")
def interest_over_time():
    kw_raw = request.args.get("kw", "AI")
    geo = request.args.get("geo", "TW")
    timeframe = request.args.get("timeframe", "today 12-m")
    kw_list = [k.strip() for k in kw_raw.split(",") if k.strip()][:5]

    try:
        pt = get_pytrends()
        pt.build_payload(kw_list=kw_list, timeframe=timeframe, geo=geo)
        df = safe_call(pt.interest_over_time)

        if df.empty:
            return jsonify({"labels": [], "datasets": []})

        if "isPartial" in df.columns:
            df = df.drop(columns=["isPartial"])

        labels = [str(d.date()) for d in df.index]
        colors = ["#6366f1", "#22d3ee", "#f59e0b", "#10b981", "#f43f5e"]
        datasets = [
            {"label": col, "data": df[col].tolist(), "color": colors[i % len(colors)]}
            for i, col in enumerate(df.columns)
        ]

        # 計算各關鍵字平均熱度作為摘要
        result_summary = {col: round(float(df[col].mean()), 1) for col in df.columns}

        # 非同步儲存歷史（不阻塞回應）
        save_query_history(kw_list, geo, timeframe, result_summary)

        return jsonify({"labels": labels, "datasets": datasets})

    except Exception as e:
        logger.error("interest-over-time error: %s", e)
        return jsonify({"error": str(e)}), 500


# ── API: Interest by Region ───────────────────────────────────
@app.route("/api/interest-by-region")
def interest_by_region():
    kw_raw = request.args.get("kw", "AI")
    geo = request.args.get("geo", "")
    timeframe = request.args.get("timeframe", "today 12-m")
    kw_list = [k.strip() for k in kw_raw.split(",") if k.strip()][:5]

    try:
        pt = get_pytrends()
        pt.build_payload(kw_list=kw_list, timeframe=timeframe, geo=geo)
        df = safe_call(pt.interest_by_region, resolution="COUNTRY",
                       inc_low_vol=True, inc_geo_code=False)

        if df.empty:
            return jsonify({"regions": []})

        first_kw = kw_list[0]
        if first_kw not in df.columns:
            return jsonify({"regions": []})

        df = df[[first_kw]].sort_values(first_kw, ascending=False).head(15)
        regions = [
            {"name": idx, "value": int(row[first_kw])}
            for idx, row in df.iterrows()
            if int(row[first_kw]) > 0
        ]
        return jsonify({"keyword": first_kw, "regions": regions})

    except Exception as e:
        logger.error("interest-by-region error: %s", e)
        return jsonify({"error": str(e)}), 500


# ── API: Related Queries ──────────────────────────────────────
@app.route("/api/related-queries")
def related_queries():
    kw_raw = request.args.get("kw", "AI")
    geo = request.args.get("geo", "TW")
    timeframe = request.args.get("timeframe", "today 3-m")
    kw_list = [k.strip() for k in kw_raw.split(",") if k.strip()][:5]

    try:
        pt = get_pytrends()
        pt.build_payload(kw_list=kw_list, timeframe=timeframe, geo=geo)
        result = safe_call(pt.related_queries)

        output = {}
        for kw in kw_list:
            kw_data = result.get(kw, {}) if result else {}
            top_df = kw_data.get("top")
            rising_df = kw_data.get("rising")

            output[kw] = {
                "top": (
                    top_df[["query", "value"]].head(10).to_dict(orient="records")
                    if top_df is not None and not top_df.empty else []
                ),
                "rising": (
                    rising_df[["query", "value"]].head(15).to_dict(orient="records")
                    if rising_df is not None and not rising_df.empty else []
                ),
            }

        # 若所有關鍵字的 rising 都為空，嘗試用較短時間窗重新抓取
        all_rising_empty = all(len(v["rising"]) == 0 for v in output.values())
        if all_rising_empty and timeframe != "now 7-d":
            logger.info("rising 全空，改用 now 7-d 重新抓取…")
            time.sleep(2)
            pt.build_payload(kw_list=kw_list, timeframe="now 7-d", geo=geo)
            result2 = safe_call(pt.related_queries)
            if result2:
                for kw in kw_list:
                    kw_data2 = result2.get(kw, {})
                    rising_df2 = kw_data2.get("rising")
                    if rising_df2 is not None and not rising_df2.empty:
                        output[kw]["rising"] = (
                            rising_df2[["query", "value"]].head(15).to_dict(orient="records")
                        )

        return jsonify(output)

    except Exception as e:
        logger.error("related-queries error: %s", e)
        return jsonify({"error": str(e)}), 500



# ── API: Keyword Suggestions ──────────────────────────────────
@app.route("/api/suggestions")
def suggestions():
    kw = request.args.get("kw", "")
    if not kw:
        return jsonify([])
    try:
        pt = get_pytrends()
        result = safe_call(pt.suggestions, keyword=kw)
        return jsonify([{"title": s["title"], "type": s.get("type", "")} for s in result[:6]])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── API: Query History ────────────────────────────────────────
@app.route("/api/history")
def query_history():
    """從 Supabase 取得最近 20 筆查詢記錄"""
    try:
        sb = get_supabase()
        resp = (
            sb.table("trend_query_history")
            .select("id, keywords, geo, timeframe, result_summary, queried_at")
            .order("queried_at", desc=True)
            .limit(20)
            .execute()
        )
        return jsonify(resp.data)
    except Exception as e:
        logger.error("history error: %s", e)
        return jsonify({"error": str(e)}), 500


# ── API: Scenarios List ─────────────────────────────────────
@app.route("/api/scenarios")
def get_scenarios():
    """回傳所有可用場景名稱清單"""
    return jsonify({"scenarios": list_scenarios()})


# ── API: Keyword Discovery ────────────────────────────────
@app.route("/api/keyword-discovery")
def keyword_discovery():
    """
    Query params:
      scenario      場景名稱（必填）
      geo           地區代碼，預設 TW
      top_n         取几個高聲量關鍵字，預設 5
      force_refresh true 則忽略快取重新從Google Trends抓取
    """
    scenario = request.args.get("scenario", "").strip()
    if not scenario:
        return jsonify({"error": "請提供 scenario 參數"}), 400

    geo = request.args.get("geo", "TW")
    top_n = int(request.args.get("top_n", 5))
    force_refresh = request.args.get("force_refresh", "false").lower() == "true"

    try:
        result = run_discovery(
            scenario=scenario,
            geo=geo,
            top_n=min(top_n, 5),   # Google Trends 上限 5
            force_refresh=force_refresh,
        )
        return jsonify(result)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error("keyword-discovery error: %s", e)
        return jsonify({"error": str(e)}), 500


# ── API: HF AI — 動態服務推測 ────────────────────────────────
@app.route("/api/ai-services")
def ai_services():
    """HF LLM 為指定關鍵字生成對應服務/商品建議。"""
    keyword = request.args.get("keyword", "").strip()
    if not keyword:
        return jsonify({"error": "請提供 keyword 參數"}), 400
    scenario = request.args.get("scenario", "")
    try:
        services = suggest_services_ai(keyword, scenario)
        return jsonify({"keyword": keyword, "services": services, "source": "ai"})
    except Exception as e:
        logger.error("ai-services error: %s", e)
        return jsonify({"error": str(e)}), 500


# ── API: HF AI — 場景分類 ─────────────────────────────────────
@app.route("/api/scenario-classify")
def scenario_classify():
    """用零樣本分類判斷關鍵字屬於哪個場景。"""
    keyword = request.args.get("keyword", "").strip()
    if not keyword:
        return jsonify({"error": "請提供 keyword 參數"}), 400
    try:
        result = classify_keyword_scenario(keyword)
        return jsonify(result)
    except Exception as e:
        logger.error("scenario-classify error: %s", e)
        return jsonify({"error": str(e)}), 500


# ── API: HF AI — 語意關鍵字搜尋 ──────────────────────────────
@app.route("/api/semantic-search")
def semantic_search():
    """計算 query 與種子關鍵字的語意相似度，回傳最近的 top_k 筆。"""
    query = request.args.get("query", "").strip()
    if not query:
        return jsonify({"error": "請提供 query 參數"}), 400
    scenario = request.args.get("scenario", "")
    top_k = min(int(request.args.get("top_k", 5)), 10)

    # 候選詞：指定場景的種子詞，或所有場景
    if scenario and scenario in SCENARIO_SEEDS:
        candidates = SCENARIO_SEEDS[scenario]
    else:
        candidates = [kw for kws in SCENARIO_SEEDS.values() for kw in kws]

    try:
        results = semantic_keyword_search(query, candidates, top_k=top_k)
        return jsonify({"query": query, "matches": results})
    except Exception as e:
        logger.error("semantic-search error: %s", e)
        return jsonify({"error": str(e)}), 500


# ── API: HF AI — 趨勢洞察摘要 ────────────────────────────────
@app.route("/api/trend-summary", methods=["POST"])
def trend_summary():
    """根據 IOT 資料用 LLM 生成趨勢洞察文字。"""
    body = request.get_json(force=True, silent=True) or {}
    keywords = body.get("keywords", [])
    iot_data = body.get("data", {})
    geo = body.get("geo", "TW")

    if not keywords:
        return jsonify({"error": "請提供 keywords 陣列"}), 400
    try:
        summary = summarize_trends(keywords, iot_data, geo)
        return jsonify({"summary": summary})
    except Exception as e:
        logger.error("trend-summary error: %s", e)
        return jsonify({"error": str(e)}), 500


# ── API: 關鍵字新跡 ──────────────────────────────────
@app.route("/api/keyword-news")
def keyword_news():
    """GNews API 取得關鍵字相關新跡。"""
    keyword = request.args.get("keyword", "").strip()
    if not keyword:
        return jsonify({"error": "請提供 keyword"}), 400
    scenario  = request.args.get("scenario", "")
    max_n     = min(int(request.args.get("max", 5)), 10)
    lang      = request.args.get("lang", "zh-Hant")
    try:
        news = fetch_keyword_news(keyword, scenario=scenario, lang=lang, max_results=max_n)
        return jsonify({"keyword": keyword, "news": news, "total": len(news)})
    except Exception as e:
        logger.error("keyword-news error: %s", e)
        return jsonify({"error": str(e)}), 500


# ── API: 商業意圖評分 ──────────────────────────────
@app.route("/api/commercial-intent")
def commercial_intent():
    """LLM 評估關鍵字商業轉換意圖。"""
    keyword = request.args.get("keyword", "").strip()
    if not keyword:
        return jsonify({"error": "請提供 keyword"}), 400
    scenario = request.args.get("scenario", "")
    try:
        result = score_commercial_intent(keyword, scenario)
        return jsonify(result)
    except Exception as e:
        logger.error("commercial-intent error: %s", e)
        return jsonify({"error": str(e)}), 500


# ── API: 平台歸因分析 ───────────────────────────────
@app.route("/api/platform-attribution")
def platform_attribution():
    """LLM 推估關鍵字在各通路/平台的曝光比重。"""
    keyword = request.args.get("keyword", "").strip()
    if not keyword:
        return jsonify({"error": "請提供 keyword"}), 400
    scenario = request.args.get("scenario", "")
    try:
        result = analyze_platform_attribution(keyword, scenario)
        return jsonify(result)
    except Exception as e:
        logger.error("platform-attribution error: %s", e)
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True, port=5050, threaded=False)
