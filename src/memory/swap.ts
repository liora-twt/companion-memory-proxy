import type { Env } from "../types";

// ---------------------------------------------------------------------------
// Swap: snapshot-based rollback for destructive nightly passes.
//
// Before a destructive pass (dream / retention / monthly rollup) runs, the
// affected tables are copied into plain `swap_<id>__<table>` snapshot tables
// (CREATE TABLE AS SELECT — constraints are intentionally NOT copied). The
// registry table `swap_snapshots` (migration 0012) tracks one row per
// snapshot. After the pass, callers compare row counts and can restore.
//
// Fail-safe contract: every public function here may throw; callers in
// src/index.ts wrap all swap calls in their own try/catch so a swap failure
// can never fail the pass itself.
// ---------------------------------------------------------------------------

// Fixed allowlist of known D1 tables — snapshot/restore validate against this
// so no arbitrary identifier ever reaches an interpolated DDL statement.
const SWAP_TABLE_ALLOWLIST = new Set([
  "messages",
  "memories",
  "memory_events",
  "summaries",
  "cache_entries",
  "processing_cursors",
  "idempotency_keys",
  "usage_logs",
  "memory_lifecycle",
  "digest",
  "precious",
  "glossary",
  "longtail",
  "daily_log",
  "memory_candidates",
  "dream_runs",
  "memory_relations",
  "perception_cache",
  "weekly_log",
  "monthly_log"
]);

const IDENTIFIER_RE = /^[A-Za-z0-9_]+$/;

// Collapse guard: auto-restore only when a pass wiped more than 20% AND more
// than 50 rows of `memories` — small drains (retention's daily delete) must
// never trigger a rollback.
const COLLAPSE_MAX_DROP_RATIO = 0.2;
const COLLAPSE_MIN_DROP_ROWS = 50;

const SNAPSHOT_MAX_AGE_DAYS = 30;

export interface SwapSnapshotRow {
  id: string;
  pass: string;
  created_at: string;
  tables_json: string;
  rowcounts_json: string;
  status: string;
}

export interface SwapSnapshotResult {
  id: string;
  createdAt: string;
  tables: string[];
  rowcounts: Record<string, number>;
}

export function isSwapEnabled(env: Env): boolean {
  return env.SWAP_ENABLED !== "false";
}

export function shouldAutoRestore(before: number, after: number): boolean {
  if (before <= 0) return false;
  return before - after > COLLAPSE_MIN_DROP_ROWS && after < before * (1 - COLLAPSE_MAX_DROP_RATIO);
}

function assertPassName(pass: string): string {
  if (!IDENTIFIER_RE.test(pass)) throw new Error(`swap: invalid pass name: ${pass}`);
  return pass;
}

function assertTableName(table: string): string {
  if (!SWAP_TABLE_ALLOWLIST.has(table)) throw new Error(`swap: table not in allowlist: ${table}`);
  return table;
}

function snapTableName(snapshotId: string, table: string): string {
  return `swap_${snapshotId}__${assertTableName(table)}`;
}

// Idempotent self-heal so cron passes stay protected even before migration
// 0012 is applied; the migration is the canonical schema source.
async function ensureSwapRegistry(db: D1Database): Promise<void> {
  await db
    .prepare(
      `CREATE TABLE IF NOT EXISTS swap_snapshots (
        id TEXT PRIMARY KEY,
        pass TEXT NOT NULL,
        created_at TEXT NOT NULL,
        tables_json TEXT NOT NULL,
        rowcounts_json TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'held'
      )`
    )
    .run();
}

export async function countRows(db: D1Database, tables: string[]): Promise<Record<string, number>> {
  const counts: Record<string, number> = {};
  for (const table of tables) {
    assertTableName(table);
    const row = await db.prepare(`SELECT COUNT(*) AS n FROM ${table}`).first<{ n: number }>();
    counts[table] = row?.n ?? 0;
  }
  return counts;
}

export async function takeSwapSnapshot(
  db: D1Database,
  passName: string,
  tables: string[]
): Promise<SwapSnapshotResult> {
  const pass = assertPassName(passName);
  if (tables.length === 0) throw new Error("swap: no tables to snapshot");
  for (const table of tables) assertTableName(table);

  const createdAt = new Date().toISOString();
  const ts = createdAt.replace(/[^0-9]/g, "");
  const rand = Math.random().toString(36).slice(2, 8);
  const id = `${pass}_${ts}_${rand}`;

  const rowcounts = await countRows(db, tables);

  await ensureSwapRegistry(db);
  const statements = tables.map((table) =>
    db.prepare(`CREATE TABLE ${snapTableName(id, table)} AS SELECT * FROM ${table}`)
  );
  statements.push(
    db
      .prepare(
        "INSERT INTO swap_snapshots (id, pass, created_at, tables_json, rowcounts_json, status) VALUES (?, ?, ?, ?, ?, 'held')"
      )
      .bind(id, pass, createdAt, JSON.stringify(tables), JSON.stringify(rowcounts))
  );
  // D1 batch is transactional. Keeping every CREATE plus the registry insert
  // in one batch prevents an interrupted snapshot from leaving physical tables
  // that no registry row can later discover or clean up.
  await db.batch(statements);

  return { id, createdAt, tables, rowcounts };
}

async function getSnapshotRow(db: D1Database, snapshotId: string): Promise<SwapSnapshotRow> {
  if (!IDENTIFIER_RE.test(snapshotId)) throw new Error(`swap: invalid snapshot id: ${snapshotId}`);
  const row = await db
    .prepare("SELECT * FROM swap_snapshots WHERE id = ?")
    .bind(snapshotId)
    .first<SwapSnapshotRow>();
  if (!row) throw new Error(`swap: snapshot not found: ${snapshotId}`);
  return row;
}

function parseSnapshottedTables(row: SwapSnapshotRow): string[] {
  const parsed: unknown = JSON.parse(row.tables_json);
  if (!Array.isArray(parsed)) throw new Error(`swap: corrupt tables_json for ${row.id}`);
  return parsed.map((table) => assertTableName(String(table)));
}

export async function restoreSwapSnapshot(
  db: D1Database,
  snapshotId: string
): Promise<{ id: string; tables: string[] }> {
  await ensureSwapRegistry(db);
  const row = await getSnapshotRow(db, snapshotId);
  const tables = parseSnapshottedTables(row);

  const statements: D1PreparedStatement[] = [];
  for (const table of tables) {
    statements.push(db.prepare(`DELETE FROM ${table}`));
    statements.push(db.prepare(`INSERT INTO ${table} SELECT * FROM ${snapTableName(snapshotId, table)}`));
  }
  statements.push(
    db.prepare("UPDATE swap_snapshots SET status = 'restored' WHERE id = ?").bind(snapshotId)
  );
  // Atomic per D1 batch semantics: either every table rolls back or none do.
  await db.batch(statements);
  // Snapshot tables are intentionally kept after restore for forensics;
  // cleanupSwapSnapshots (or POST /admin/swap/drop) reclaims them later.
  return { id: snapshotId, tables };
}

// Finds physical snapshot tables for one id via sqlite_master (D1 has no
// reliable PRAGMA table_list), so partially-written snapshots still drop
// cleanly even when tables_json is incomplete.
async function listPhysicalSnapTables(db: D1Database, snapshotId: string): Promise<string[]> {
  const escaped = snapshotId.replace(/_/g, "\\_");
  const { results } = await db
    .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE ? ESCAPE '\\'")
    .bind(`swap\\_${escaped}\\_\\_%`)
    .all<{ name: string }>();
  return (results ?? [])
    .map((row) => row.name)
    .filter((name) => IDENTIFIER_RE.test(name) && name.startsWith(`swap_${snapshotId}__`));
}

export async function dropSwapSnapshot(
  db: D1Database,
  snapshotId: string
): Promise<{ id: string; droppedTables: string[] }> {
  await ensureSwapRegistry(db);
  const row = await getSnapshotRow(db, snapshotId);

  const names = new Set<string>();
  for (const table of parseSnapshottedTables(row)) names.add(snapTableName(snapshotId, table));
  for (const name of await listPhysicalSnapTables(db, snapshotId)) names.add(name);

  for (const name of names) {
    await db.prepare(`DROP TABLE IF EXISTS ${name}`).run();
  }
  await db.prepare("UPDATE swap_snapshots SET status = 'dropped' WHERE id = ?").bind(snapshotId).run();
  return { id: snapshotId, droppedTables: [...names] };
}

export async function listSwapSnapshots(db: D1Database, limit = 50): Promise<SwapSnapshotRow[]> {
  await ensureSwapRegistry(db);
  const { results } = await db
    .prepare("SELECT * FROM swap_snapshots ORDER BY created_at DESC LIMIT ?")
    .bind(limit)
    .all<SwapSnapshotRow>();
  return results ?? [];
}

export async function cleanupSwapSnapshots(
  db: D1Database,
  keepPerPass = 7
): Promise<{ dropped: string[] }> {
  await ensureSwapRegistry(db);
  const { results } = await db
    .prepare("SELECT * FROM swap_snapshots WHERE status != 'dropped' ORDER BY created_at DESC")
    .all<SwapSnapshotRow>();

  const cutoff = new Date(Date.now() - SNAPSHOT_MAX_AGE_DAYS * 86_400_000).toISOString();
  const seenPerPass = new Map<string, number>();
  const toDrop: string[] = [];

  for (const row of results ?? []) {
    const seen = seenPerPass.get(row.pass) ?? 0;
    seenPerPass.set(row.pass, seen + 1);
    if (seen >= keepPerPass || row.created_at < cutoff) toDrop.push(row.id);
  }

  const dropped: string[] = [];
  for (const id of toDrop) {
    try {
      await dropSwapSnapshot(db, id);
      dropped.push(id);
    } catch (error) {
      console.error("swap: failed to drop snapshot during cleanup", { id, error: String(error) });
    }
  }
  return { dropped };
}
