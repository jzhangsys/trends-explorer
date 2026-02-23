"""
hf_services.py
==============
Hugging Face Inference API 整合模組

提供四項 AI 功能：
  1. suggest_services_ai()    — 動態生成關鍵字對應服務/商品（LLM）
  2. classify_keyword_scenario() — 零樣本場景分類（Zero-shot）
  3. semantic_keyword_search()   — 語意關鍵字搜尋（句子嵌入）
  4. summarize_trends()          — AI 趨勢洞察摘要（LLM）

所有結果快取於記憶體（TTL 1 小時），API 錯誤時優雅降級。
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# 設定
# ─────────────────────────────────────────────────────────────
HF_TOKEN: str = os.environ.get("HF_TOKEN", "")
HF_API_BASE = "https://api-inference.huggingface.co/models"

MODELS = {
    "llm":        "Qwen/Qwen2.5-72B-Instruct",
    "zeroshot":   "MoritzLaurer/mDeBERTa-v3-base-mnli-xnli",
    "embedding":  "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
}

CACHE_TTL_SECONDS = 3600   # 1 小時
_cache: dict[str, dict] = {}   # key → { "data": ..., "expires_at": float }


# ─────────────────────────────────────────────────────────────
# 快取工具
# ─────────────────────────────────────────────────────────────

def _cache_get(key: str):
    entry = _cache.get(key)
    if entry and time.time() < entry["expires_at"]:
        return entry["data"]
    return None


def _cache_set(key: str, data):
    _cache[key] = {"data": data, "expires_at": time.time() + CACHE_TTL_SECONDS}


# ─────────────────────────────────────────────────────────────
# HF API 通用請求
# ─────────────────────────────────────────────────────────────

def _hf_post(model_id: str, payload: dict, timeout: int = 30) -> Optional[dict | list]:
    """向 HF Inference API 發送 POST 請求。"""
    url = f"{HF_API_BASE}/{model_id}"
    headers = {"Authorization": f"Bearer {HF_TOKEN}", "Content-Type": "application/json"}
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 503:
            # 模型冷啟動，等待重試
            logger.warning("HF model %s loading, waiting 15s…", model_id)
            time.sleep(15)
            resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
            return resp.json() if resp.status_code == 200 else None
        else:
            logger.warning("HF API %s error %s: %s", model_id, resp.status_code, resp.text[:200])
            return None
    except Exception as exc:
        logger.error("HF API request failed: %s", exc)
        return None


# ─────────────────────────────────────────────────────────────
# 1. 動態服務/商品推測 (LLM)
# ─────────────────────────────────────────────────────────────

def suggest_services_ai(keyword: str, scenario: str = "") -> list[str]:
    """
    利用 LLM 為關鍵字生成 3 個對應的服務或商品名稱（繁體中文）。

    Returns:
        list[str] — 最多 3 個服務/商品名稱；失敗時回傳空 list
    """
    cache_key = f"suggest:{keyword}:{scenario}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    scenario_hint = f"（場景：{scenario}）" if scenario else ""
    prompt = (
        f"<|im_start|>system\n你是台灣市場行銷專家，請用繁體中文回答。<|im_end|>\n"
        f"<|im_start|>user\n"
        f"關鍵字「{keyword}」{scenario_hint} 在 Google 上被大量搜尋。"
        f"請列出消費者搜尋這個關鍵字時，最可能想要找的 3 個具體服務或商品名稱。"
        f"只輸出 JSON 陣列，例如：[\"服務A\",\"服務B\",\"服務C\"]，不要其他說明。"
        f"<|im_end|>\n<|im_start|>assistant\n"
    )

    result = _hf_post(
        MODELS["llm"],
        {
            "inputs": prompt,
            "parameters": {
                "max_new_tokens": 80,
                "temperature": 0.3,
                "return_full_text": False,
            },
        },
        timeout=30,
    )

    services: list[str] = []
    try:
        if result and isinstance(result, list):
            text = result[0].get("generated_text", "")
            # 提取 JSON 陣列
            start = text.find("[")
            end = text.rfind("]") + 1
            if start != -1 and end > start:
                services = json.loads(text[start:end])
                services = [str(s).strip() for s in services if s][:3]
    except Exception as exc:
        logger.warning("suggest_services_ai parse error: %s", exc)

    _cache_set(cache_key, services)
    return services


# ─────────────────────────────────────────────────────────────
# 2. 關鍵字場景分類 (Zero-shot Classification)
# ─────────────────────────────────────────────────────────────

SCENARIO_LABELS_ZH = {
    "旅遊": "travel and tourism",
    "健康": "health and wellness",
    "牙科": "dental care and orthodontics",
    "保健品": "dietary supplements and nutrition",
}


def classify_keyword_scenario(keyword: str) -> dict:
    """
    用零樣本分類判斷關鍵字屬於哪個場景，回傳最高信心的場景及分數。

    Returns:
        {"scenario": "牙科", "confidence": 0.93, "all_scores": {...}}
    """
    cache_key = f"classify:{keyword}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    candidate_labels = list(SCENARIO_LABELS_ZH.values())
    result = _hf_post(
        MODELS["zeroshot"],
        {
            "inputs": keyword,
            "parameters": {"candidate_labels": candidate_labels},
        },
        timeout=20,
    )

    output = {"scenario": "", "confidence": 0.0, "all_scores": {}}
    try:
        if result and "labels" in result and "scores" in result:
            # 反查中文場景名稱
            en_to_zh = {v: k for k, v in SCENARIO_LABELS_ZH.items()}
            all_scores = {
                en_to_zh.get(lbl, lbl): round(score, 4)
                for lbl, score in zip(result["labels"], result["scores"])
            }
            top_en = result["labels"][0]
            output = {
                "scenario": en_to_zh.get(top_en, top_en),
                "confidence": round(result["scores"][0], 4),
                "all_scores": all_scores,
            }
    except Exception as exc:
        logger.warning("classify_keyword_scenario parse error: %s", exc)

    _cache_set(cache_key, output)
    return output


# ─────────────────────────────────────────────────────────────
# 3. 語意關鍵字搜尋 (Sentence Embeddings)
# ─────────────────────────────────────────────────────────────

import math


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    return dot / (norm_a * norm_b + 1e-9)


def _get_embedding(texts: list[str]) -> Optional[list[list[float]]]:
    """取得句子嵌入向量。"""
    result = _hf_post(
        MODELS["embedding"],
        {"inputs": texts},
        timeout=20,
    )
    if result and isinstance(result, list) and len(result) > 0:
        return result
    return None


def semantic_keyword_search(query: str, candidates: list[str], top_k: int = 5) -> list[dict]:
    """
    對 query 計算與 candidates 的語意相似度，回傳最相近的 top_k 筆。

    Returns:
        [{"keyword": "隱適美", "score": 0.87}, ...]  按 score 降序排列
    """
    cache_key = f"semantic:{query}:{','.join(candidates[:10])}:{top_k}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    all_texts = [query] + candidates
    embeddings = _get_embedding(all_texts)

    results: list[dict] = []
    if embeddings and len(embeddings) > 1:
        query_vec = embeddings[0]
        for i, cand in enumerate(candidates):
            if i + 1 < len(embeddings):
                score = _cosine_similarity(query_vec, embeddings[i + 1])
                results.append({"keyword": cand, "score": round(score, 4)})
        results.sort(key=lambda x: x["score"], reverse=True)
        results = results[:top_k]

    _cache_set(cache_key, results)
    return results


# ─────────────────────────────────────────────────────────────
# 4. AI 趨勢洞察摘要 (LLM)
# ─────────────────────────────────────────────────────────────

def summarize_trends(keywords: list[str], iot_data: dict, geo: str = "TW") -> str:
    """
    根據 Interest Over Time 資料生成繁體中文趨勢洞察摘要。

    Args:
        keywords: 查詢的關鍵字列表
        iot_data: /api/interest-over-time 回傳的資料
        geo:      地區代碼

    Returns:
        str — 2-3 句的中文趨勢洞察，失敗時回傳空字串
    """
    cache_key = f"summary:{','.join(keywords)}:{geo}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    # 整理資料摘要給 LLM
    datasets = iot_data.get("datasets", [])
    data_desc_parts = []
    for ds in datasets:
        label = ds.get("label", "")
        data = ds.get("data", [])
        if data:
            avg = round(sum(data) / len(data), 1)
            peak = max(data)
            data_desc_parts.append(f"{label}（平均聲量={avg}，峰值={peak}）")
    data_desc = "；".join(data_desc_parts) if data_desc_parts else "數據不足"

    geo_map = {"TW": "台灣", "US": "美國", "JP": "日本", "": "全球"}
    geo_name = geo_map.get(geo, geo)

    prompt = (
        f"<|im_start|>system\n你是數位行銷分析師，請用繁體中文回答，語氣專業簡潔。<|im_end|>\n"
        f"<|im_start|>user\n"
        f"以下是 {geo_name} Google Trends 資料（近期）：{data_desc}。"
        f"請用 2-3 句話說明這些關鍵字的搜尋趨勢洞察，並給出一個行銷建議。"
        f"<|im_end|>\n<|im_start|>assistant\n"
    )

    result = _hf_post(
        MODELS["llm"],
        {
            "inputs": prompt,
            "parameters": {
                "max_new_tokens": 150,
                "temperature": 0.5,
                "return_full_text": False,
            },
        },
        timeout=35,
    )

    summary = ""
    try:
        if result and isinstance(result, list):
            summary = result[0].get("generated_text", "").strip()
            # 截斷到第一個自然結尾
            for end_mark in ["。\n", "。", "\n\n"]:
                idx = summary.rfind(end_mark)
                if idx > 20:
                    summary = summary[:idx + len(end_mark)].strip()
                    break
    except Exception as exc:
        logger.warning("summarize_trends parse error: %s", exc)

    _cache_set(cache_key, summary)
    return summary
