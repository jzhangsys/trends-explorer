-- ============================================================
-- keyword_snapshots 表格建立 SQL
-- 請在 Supabase 專案的 SQL Editor 執行此段
-- ============================================================

create table if not exists keyword_snapshots (
  id            bigint generated always as identity primary key,
  scenario      text        not null,
  geo           text        not null default 'TW',
  top_keywords  jsonb       not null,   -- [{keyword, avg_score}, ...]
  related_kws   jsonb       not null,   -- [{keyword, source, type, value}, ...]
  created_at    timestamptz not null default now()
);

-- 加快場景查詢（按場景 + 地區 + 建立時間倒序排列）
create index if not exists idx_kw_snap_lookup
  on keyword_snapshots (scenario, geo, created_at desc);

-- ============================================================
-- 驗證：查詢最近 10 筆快照
-- select scenario, geo, created_at, jsonb_array_length(top_keywords) as top_count
-- from keyword_snapshots
-- order by created_at desc
-- limit 10;
-- ============================================================
