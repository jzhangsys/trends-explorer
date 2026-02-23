"""
news_service.py
===============
新聞資料擷取服務

優先使用 GNews API（每日 100 次免費），
無 API Key 時 fallback 到台灣新聞 RSS feeds。
結果快取 2 小時。
"""

from __future__ import annotations

import logging
import os
import time
from typing import Optional
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

GNEWS_API_KEY: str = os.environ.get("GNEWS_API_KEY", "")
GNEWS_BASE = "https://gnews.io/api/v4/search"

# RSS fallback feeds（台灣主流媒體）
RSS_FEEDS = {
    "旅遊":  "https://www.ltn.com.tw/rss/life.xml",
    "健康":  "https://www.ltn.com.tw/rss/health.xml",
    "牙科":  "https://www.ltn.com.tw/rss/health.xml",
    "保健品": "https://www.ltn.com.tw/rss/health.xml",
    "_default": "https://www.ltn.com.tw/rss/all.xml",
}

CACHE_TTL = 7200   # 2 小時
_cache: dict[str, dict] = {}


def _cache_get(key: str):
    entry = _cache.get(key)
    if entry and time.time() < entry["expires_at"]:
        return entry["data"]
    return None


def _cache_set(key: str, data):
    _cache[key] = {"data": data, "expires_at": time.time() + CACHE_TTL}


# ─────────────────────────────────────────────────────────────
# GNews API
# ─────────────────────────────────────────────────────────────

def _fetch_gnews(keyword: str, lang: str = "zh-Hant", max_results: int = 5) -> list[dict]:
    """呼叫 GNews API 搜尋關鍵字相關新聞。"""
    if not GNEWS_API_KEY:
        return []
    try:
        params = {
            "q": keyword,
            "lang": lang,
            "country": "tw",
            "max": max_results,
            "apikey": GNEWS_API_KEY,
        }
        resp = requests.get(GNEWS_BASE, params=params, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            articles = data.get("articles", [])
            return [
                {
                    "title":       a.get("title", ""),
                    "description": a.get("description", ""),
                    "url":         a.get("url", ""),
                    "source":      a.get("source", {}).get("name", ""),
                    "published_at": a.get("publishedAt", ""),
                }
                for a in articles
            ]
        else:
            logger.warning("GNews API error %s: %s", resp.status_code, resp.text[:200])
    except Exception as exc:
        logger.error("GNews request failed: %s", exc)
    return []


# ─────────────────────────────────────────────────────────────
# RSS fallback
# ─────────────────────────────────────────────────────────────

def _fetch_rss(keyword: str, scenario: str = "", max_results: int = 5) -> list[dict]:
    """從 RSS feed 爬取並過濾含關鍵字的新聞。"""
    import xml.etree.ElementTree as ET

    feed_url = RSS_FEEDS.get(scenario, RSS_FEEDS["_default"])
    try:
        resp = requests.get(feed_url, timeout=10, headers={"User-Agent": "TrendsExplorer/1.0"})
        if resp.status_code != 200:
            return []
        root = ET.fromstring(resp.content)
        items = root.findall(".//item")
        results = []
        for item in items:
            title = item.findtext("title", "")
            desc  = item.findtext("description", "")
            url   = item.findtext("link", "")
            pub   = item.findtext("pubDate", "")
            # 簡單關鍵字過濾
            if keyword in title or keyword in desc:
                results.append({
                    "title":       title,
                    "description": desc[:200] if desc else "",
                    "url":         url,
                    "source":      "自由時報",
                    "published_at": pub,
                })
            if len(results) >= max_results:
                break
        return results
    except Exception as exc:
        logger.error("RSS fetch failed: %s", exc)
    return []


# ─────────────────────────────────────────────────────────────
# 公開介面
# ─────────────────────────────────────────────────────────────

def fetch_keyword_news(
    keyword: str,
    scenario: str = "",
    lang: str = "zh-Hant",
    max_results: int = 5,
) -> list[dict]:
    """
    擷取關鍵字相關台灣新聞。

    優先 GNews API → 無 key 或失敗時 fallback RSS。
    快取 2 小時。

    Returns:
        [{title, description, url, source, published_at}, ...]
    """
    cache_key = f"news:{keyword}:{scenario}:{max_results}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    # 優先 GNews
    news = _fetch_gnews(keyword, lang=lang, max_results=max_results)

    # fallback: RSS
    if not news:
        news = _fetch_rss(keyword, scenario=scenario, max_results=max_results)

    # fallback: 提供空列表（前端顯示「暫無新聞」）
    _cache_set(cache_key, news)
    return news
