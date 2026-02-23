---
title: Trends Explorer
emoji: 📈
colorFrom: blue
colorTo: indigo
sdk: docker
pinned: false
license: mit
---

# 🔍 Google Trends Explorer

Google 關鍵字趨勢分析工具，支援：
- 搜尋熱度趨勢圖（Interest Over Time）
- 地區分佈分析（Interest by Region）
- 相關查詢（Related Queries / Rising）
- 🎯 高聲量關鍵字發現（旅遊 / 健康 / 牙科 / 保健品）
- 🛍 服務/商品推測標籤

## 技術架構
- **後端**：Flask + pytrends
- **資料庫**：Supabase (PostgreSQL)
- **部署**：Hugging Face Spaces (Docker)
