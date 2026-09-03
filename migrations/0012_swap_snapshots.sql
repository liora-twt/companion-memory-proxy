-- swap_snapshots: registry for snapshot-based rollback ("swap"). Destructive
-- nightly passes (dream / retention / monthly rollup) copy affected tables into
-- swap_<id>__<table> tables first; this registry tracks them for restore/drop/
-- cleanup. status: held → restored | dropped.

CREATE TABLE IF NOT EXISTS swap_snapshots (
  id TEXT PRIMARY KEY,
  pass TEXT NOT NULL,
  created_at TEXT NOT NULL,
  tables_json TEXT NOT NULL,
  rowcounts_json TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'held'
);
