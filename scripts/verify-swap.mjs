#!/usr/bin/env node
/**
 * Contract + behavioral tests for the swap snapshot rollback module.
 *
 * Behavioral tests import the real src/memory/swap.ts (tsx) against a minimal
 * in-memory D1 stub that understands exactly the SQL shapes swap.ts emits.
 *
 * Run:  npx tsx scripts/verify-swap.mjs
 * Exit 0 = all checks passed, exit 1 = failure.
 */

import { strict as assert } from "node:assert";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

import {
  cleanupSwapSnapshots,
  countRows,
  dropSwapSnapshot,
  isSwapEnabled,
  listSwapSnapshots,
  restoreSwapSnapshot,
  shouldAutoRestore,
  takeSwapSnapshot
} from "../src/memory/swap.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

// ---------------------------------------------------------------------------
// Static source contract checks
// ---------------------------------------------------------------------------

const swapSource = readFileSync(resolve(root, "src/memory/swap.ts"), "utf8");
const indexSource = readFileSync(resolve(root, "src/index.ts"), "utf8");
const migrationSource = readFileSync(resolve(root, "migrations/0012_swap_snapshots.sql"), "utf8");
const wranglerSource = readFileSync(resolve(root, "wrangler.toml"), "utf8");

assert.match(swapSource, /export async function takeSwapSnapshot/);
assert.match(swapSource, /export async function restoreSwapSnapshot/);
assert.match(swapSource, /export async function dropSwapSnapshot/);
assert.match(swapSource, /export async function cleanupSwapSnapshots/);
assert.match(swapSource, /export async function countRows/);
assert.match(swapSource, /SWAP_TABLE_ALLOWLIST/);
assert.match(swapSource, /sqlite_master/);
assert.doesNotMatch(swapSource, /PRAGMA \w+\(/);

assert.match(indexSource, /\/admin\/swap\/list/);
assert.match(indexSource, /\/admin\/swap\/restore/);
assert.match(indexSource, /\/admin\/swap\/drop/);
assert.match(indexSource, /isSwapEnabled/);
assert.match(indexSource, /takeSwapGuard\(env, "dream"/);
assert.match(indexSource, /takeSwapGuard\(env, "retention"/);
assert.match(indexSource, /takeSwapGuard\(env, "monthly"/);
assert.match(indexSource, /cleanupSwapSnapshots/);

assert.match(migrationSource, /CREATE TABLE IF NOT EXISTS swap_snapshots/);
assert.match(migrationSource, /status TEXT NOT NULL DEFAULT 'held'/);

assert.match(wranglerSource, /SWAP_ENABLED/);

// ---------------------------------------------------------------------------
// Minimal in-memory D1 stub — only the SQL shapes swap.ts emits
// ---------------------------------------------------------------------------

function createMockD1() {
  // table name -> array of row objects (plain copies)
  const tables = new Map();
  const ensure = (name) => {
    if (!tables.has(name)) tables.set(name, []);
    return tables.get(name);
  };
  ensure("swap_snapshots");

  // LIKE pattern -> RegExp: \_ is a literal underscore, % is .*, _ is any char
  function likeToRegExp(pattern) {
    let out = "";
    for (let i = 0; i < pattern.length; i += 1) {
      const ch = pattern[i];
      if (ch === "\\" && pattern[i + 1] === "_") {
        out += "_";
        i += 1;
      } else if (ch === "%") {
        out += ".*";
      } else if (ch === "_") {
        out += ".";
      } else {
        out += ch;
      }
    }
    return new RegExp("^" + out + "$");
  }

  function prepare(sql) {
    const normalized = sql.replace(/\s+/g, " ").trim();
    let binds = [];
    const stmt = {
      bind(...args) {
        binds = args;
        return stmt;
      },
      async run() {
        let m;
        if ((m = normalized.match(/^CREATE TABLE IF NOT EXISTS (\w+)/))) {
          ensure(m[1]);
          return { meta: { changes: 0 } };
        }
        if ((m = normalized.match(/^CREATE TABLE (\w+) AS SELECT \* FROM (\w+)$/))) {
          tables.set(m[1], ensure(m[2]).map((row) => ({ ...row })));
          return { meta: { changes: 0 } };
        }
        if (normalized.startsWith("INSERT INTO swap_snapshots")) {
          const [id, pass, created_at, tables_json, rowcounts_json] = binds;
          ensure("swap_snapshots").push({ id, pass, created_at, tables_json, rowcounts_json, status: "held" });
          return { meta: { changes: 1 } };
        }
        if ((m = normalized.match(/^UPDATE swap_snapshots SET status = '(\w+)' WHERE id = \?$/))) {
          const row = ensure("swap_snapshots").find((r) => r.id === binds[0]);
          if (row) row.status = m[1];
          return { meta: { changes: row ? 1 : 0 } };
        }
        if ((m = normalized.match(/^DELETE FROM (\w+)$/))) {
          const rows = ensure(m[1]);
          const changes = rows.length;
          tables.set(m[1], []);
          return { meta: { changes } };
        }
        if ((m = normalized.match(/^INSERT INTO (\w+) SELECT \* FROM (\w+)$/))) {
          const source = ensure(m[2]);
          tables.set(m[1], [...ensure(m[1]), ...source.map((row) => ({ ...row }))]);
          return { meta: { changes: source.length } };
        }
        if ((m = normalized.match(/^DROP TABLE IF EXISTS (\w+)$/))) {
          tables.delete(m[1]);
          return { meta: { changes: 0 } };
        }
        throw new Error(`mock D1: unsupported run() SQL: ${normalized}`);
      },
      async first() {
        let m;
        if ((m = normalized.match(/^SELECT COUNT\(\*\) AS n FROM (\w+)$/))) {
          return { n: ensure(m[1]).length };
        }
        if (normalized === "SELECT * FROM swap_snapshots WHERE id = ?") {
          return ensure("swap_snapshots").find((r) => r.id === binds[0]) ?? null;
        }
        throw new Error(`mock D1: unsupported first() SQL: ${normalized}`);
      },
      async all() {
        if (normalized === "SELECT * FROM swap_snapshots ORDER BY created_at DESC LIMIT ?") {
          return {
            results: [...ensure("swap_snapshots")]
              .sort((a, b) => b.created_at.localeCompare(a.created_at))
              .slice(0, binds[0])
          };
        }
        if (normalized === "SELECT * FROM swap_snapshots WHERE status != 'dropped' ORDER BY created_at DESC") {
          return {
            results: [...ensure("swap_snapshots")]
              .filter((r) => r.status !== "dropped")
              .sort((a, b) => b.created_at.localeCompare(a.created_at))
          };
        }
        if (normalized.startsWith("SELECT name FROM sqlite_master")) {
          const re = likeToRegExp(binds[0]);
          return { results: [...tables.keys()].filter((name) => re.test(name)).map((name) => ({ name })) };
        }
        throw new Error(`mock D1: unsupported all() SQL: ${normalized}`);
      }
    };
    return stmt;
  }

  const mock = {
    prepare,
    _batchCalls: 0,
    async batch(stmts) {
      mock._batchCalls += 1;
      const results = [];
      for (const s of stmts) results.push(await s.run());
      return results;
    },
    _tables: tables
  };
  return mock;
}

function seedTable(db, name, rows) {
  db._tables.set(name, rows.map((row) => ({ ...row })));
}

// ---------------------------------------------------------------------------
// Behavioral tests
// ---------------------------------------------------------------------------

// 1. Snapshot creates snapshot tables + registry row with rowcounts
{
  const db = createMockD1();
  seedTable(db, "memories", [{ id: "m1" }, { id: "m2" }, { id: "m3" }]);
  seedTable(db, "daily_log", [{ id: "d1" }]);

  const snap = await takeSwapSnapshot(db, "dream", ["memories", "daily_log"]);
  assert.match(snap.id, /^dream_\d+_[a-z0-9]+$/);
  assert.deepEqual(snap.rowcounts, { memories: 3, daily_log: 1 });

  assert.equal(db._tables.get(`swap_${snap.id}__memories`).length, 3);
  assert.equal(db._tables.get(`swap_${snap.id}__daily_log`).length, 1);

  const registry = db._tables.get("swap_snapshots");
  assert.equal(registry.length, 1);
  assert.equal(registry[0].id, snap.id);
  assert.equal(registry[0].pass, "dream");
  assert.equal(registry[0].status, "held");
  assert.deepEqual(JSON.parse(registry[0].tables_json), ["memories", "daily_log"]);
  assert.equal(db._batchCalls, 1, "snapshot tables and registry row must share one atomic batch");
}

// 2. Restore reverts mutated rows, marks restored, keeps snapshot tables
{
  const db = createMockD1();
  seedTable(db, "memories", [{ id: "m1", content: "a" }, { id: "m2", content: "b" }]);
  const snap = await takeSwapSnapshot(db, "retention", ["memories"]);

  // simulate a destructive pass
  db._tables.set("memories", [{ id: "m9", content: "corrupted" }]);

  const restored = await restoreSwapSnapshot(db, snap.id);
  assert.deepEqual(restored.tables, ["memories"]);
  assert.deepEqual(db._tables.get("memories"), [
    { id: "m1", content: "a" },
    { id: "m2", content: "b" }
  ]);
  assert.equal(db._tables.get("swap_snapshots")[0].status, "restored");
  // forensic: snapshot table survives restore
  assert.equal(db._tables.get(`swap_${snap.id}__memories`).length, 2);
}

// 3. Drop removes snapshot tables, marks dropped
{
  const db = createMockD1();
  seedTable(db, "memories", [{ id: "m1" }]);
  const snap = await takeSwapSnapshot(db, "dream", ["memories"]);
  const dropResult = await dropSwapSnapshot(db, snap.id);
  assert.ok(!db._tables.has(`swap_${snap.id}__memories`));
  assert.deepEqual(dropResult.droppedTables, [`swap_${snap.id}__memories`]);
  assert.equal(db._tables.get("swap_snapshots")[0].status, "dropped");
}

// 4. Cleanup keeps newest 7 per pass and drops >30-day-old ones
{
  const db = createMockD1();
  seedTable(db, "memories", [{ id: "m1" }]);
  const registry = db._tables.get("swap_snapshots");

  // 9 held snapshots for pass "retention", one hour apart
  for (let i = 0; i < 9; i += 1) {
    const id = `retention_fake${i}`;
    const created_at = new Date(Date.now() - i * 3_600_000).toISOString();
    registry.push({ id, pass: "retention", created_at, tables_json: '["memories"]', rowcounts_json: "{}", status: "held" });
    db._tables.set(`swap_${id}__memories`, [{ id: "m1" }]);
  }
  // 1 snapshot for another pass — must survive (per-pass budget)
  registry.push({
    id: "dream_fake0",
    pass: "dream",
    created_at: new Date().toISOString(),
    tables_json: '["memories"]',
    rowcounts_json: "{}",
    status: "held"
  });
  db._tables.set("swap_dream_fake0__memories", [{ id: "m1" }]);
  // 1 snapshot from 40 days ago — must be dropped regardless of per-pass count
  registry.push({
    id: "dream_old",
    pass: "dream",
    created_at: new Date(Date.now() - 40 * 86_400_000).toISOString(),
    tables_json: '["memories"]',
    rowcounts_json: "{}",
    status: "held"
  });
  db._tables.set("swap_dream_old__memories", [{ id: "m1" }]);

  const { dropped } = await cleanupSwapSnapshots(db, 7);
  assert.deepEqual(dropped.sort(), ["dream_old", "retention_fake7", "retention_fake8"]);
  assert.equal(db._tables.get("swap_snapshots").filter((r) => r.status === "held").length, 8); // 7 retention + 1 dream
  assert.ok(!db._tables.has("swap_retention_fake8__memories"));
  assert.ok(!db._tables.has("swap_dream_old__memories"));
  assert.ok(db._tables.has("swap_retention_fake0__memories"));
  assert.ok(db._tables.has("swap_dream_fake0__memories"));
}

// 5. Collapse detection threshold: >20% AND >50 rows
{
  assert.equal(shouldAutoRestore(200, 100), true); // 100 rows dropped, 50% — collapse
  assert.equal(shouldAutoRestore(1000, 700), true); // 300 rows, 30%
  assert.equal(shouldAutoRestore(60, 0), true); // 60 rows dropped (>50), 100%
  assert.equal(shouldAutoRestore(51, 0), true); // just over the 50-row floor
  assert.equal(shouldAutoRestore(50, 0), false); // exactly 50 rows is not "more than"
  assert.equal(shouldAutoRestore(100, 79), false); // 21% dropped but only 21 rows — under the row floor
  assert.equal(shouldAutoRestore(200, 159), false); // 20.5% dropped but only 41 rows — under the row floor
  assert.equal(shouldAutoRestore(200, 160), false); // exactly 20% is not "more than"
  assert.equal(shouldAutoRestore(0, 0), false); // empty before — nothing to protect
  assert.equal(shouldAutoRestore(100, 100), false); // no change
}

// 6. Allowlist rejects bogus table names and bad ids
{
  const db = createMockD1();
  await assert.rejects(() => takeSwapSnapshot(db, "dream", ["memories; DROP TABLE memories--"]), /allowlist/);
  await assert.rejects(() => takeSwapSnapshot(db, "dream", ["nope"]), /allowlist/);
  await assert.rejects(() => takeSwapSnapshot(db, "bad pass!", ["memories"]), /invalid pass/);
  await assert.rejects(() => countRows(db, ["sqlite_master"]), /allowlist/);
  await assert.rejects(() => restoreSwapSnapshot(db, "missing_snapshot"), /not found/);
  await assert.rejects(() => restoreSwapSnapshot(db, "x'; DROP TABLE swap_snapshots--"), /invalid snapshot id/);
}

// 7. countRows + listSwapSnapshots + flag helper
{
  const db = createMockD1();
  seedTable(db, "memories", [{ id: "m1" }, { id: "m2" }]);
  seedTable(db, "weekly_log", []);
  assert.deepEqual(await countRows(db, ["memories", "weekly_log"]), { memories: 2, weekly_log: 0 });

  await takeSwapSnapshot(db, "dream", ["memories"]);
  await takeSwapSnapshot(db, "retention", ["memories"]);
  const listed = await listSwapSnapshots(db, 50);
  assert.equal(listed.length, 2);
  assert.ok(listed[0].created_at >= listed[1].created_at);

  assert.equal(isSwapEnabled({}), true);
  assert.equal(isSwapEnabled({ SWAP_ENABLED: "true" }), true);
  assert.equal(isSwapEnabled({ SWAP_ENABLED: "false" }), false);
}

console.log("verify-swap: all checks passed");
