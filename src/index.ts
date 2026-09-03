import {
  handleAdmin,
  handleDiaryAdmin,
  handleDiaryRewriteAdmin,
  handleMonthlyRollupAdmin,
  handleStarmap,
  handleSwapDropAdmin,
  handleSwapListAdmin,
  handleSwapRestoreAdmin,
  handleWeeklyRollupAdmin,
  handleWeeklyApproveAdmin
} from "./api/admin";
import { handleHealth } from "./api/health";
import { handleCache } from "./api/cache";
import { handleCacheHealth, handleVectorDoctor, handleVectorHealth, handleVectorReindex } from "./api/debug";
import { handleDreamHarvest, handleDreamRun, handleDreamStatus } from "./api/dream";
import { handleChatCompletions } from "./api/chatCompletions";
import { handleGuideDogChatCompletions } from "./api/guideDog";
import {
  handleGlossaryApi,
  handleIngestMessagesApi,
  handleMemories,
  handleMemoryBoot,
  handleMemoryCandidates,
  handleLongtailApi,
  handlePrecious,
  handleSearchMemoriesApi,
  handleDiaryApi
} from "./api/memories";
import { handleMcp } from "./api/mcp";
import { handleModels } from "./api/models";
import { handleRelationsGraph } from "./api/relations";
import { runDailyMemoryDigest, runDreamBackfill } from "./memory/dailyDigest";
import {
  runDiaryTrigger,
  runGithubDailyTrigger,
  runMonthlyRollupTrigger,
  runWeeklyRollupTrigger
} from "./memory/dream/rollupPhase";
import { runMemoryRetention } from "./memory/retention";
import {
  cleanupSwapSnapshots,
  countRows,
  isSwapEnabled,
  restoreSwapSnapshot,
  shouldAutoRestore,
  takeSwapSnapshot
} from "./memory/swap";
import { handleQueueMessage } from "./queue/consumer";
import type { Env, QueueMessage } from "./types";
import { openAiError } from "./utils/json";

const DAILY_MAINTENANCE_CRON = "10 20 * * *";

// Swap: snapshot tables before each destructive nightly pass so a bad pass can
// be rolled back (auto on memories collapse, manual via /admin/swap/*).
const SWAP_DREAM_TABLES = ["memories", "memory_candidates", "memory_relations", "daily_log", "memory_lifecycle"];
const SWAP_RETENTION_TABLES = ["messages", "usage_logs", "memory_events", "idempotency_keys", "memories"];
const SWAP_MONTHLY_TABLES = ["weekly_log", "monthly_log"];

interface SwapGuard {
  snapshotId: string;
  pass: string;
  memoriesBefore: number | null;
}

// 快照失败绝不拖垮主流程：出错只记日志并返回 null，主 pass 照常裸跑。
async function takeSwapGuard(env: Env, pass: string, tables: string[]): Promise<SwapGuard | null> {
  if (!isSwapEnabled(env)) return null;
  try {
    const memoriesBefore = tables.includes("memories") ? (await countRows(env.DB, ["memories"])).memories : null;
    const snapshot = await takeSwapSnapshot(env.DB, pass, tables);
    return { snapshotId: snapshot.id, pass, memoriesBefore };
  } catch (error) {
    console.error("swap snapshot failed; pass continues unprotected", { pass, error: String(error) });
    return null;
  }
}

// 崩坍闸：pass 后 memories 掉超 20% 且超 50 行 → 自动回滚；否则留快照给 retention 策略。
async function checkSwapCollapse(env: Env, guard: SwapGuard | null): Promise<void> {
  if (!guard || guard.memoriesBefore === null) return;
  try {
    const memoriesAfter = (await countRows(env.DB, ["memories"])).memories;
    if (!shouldAutoRestore(guard.memoriesBefore, memoriesAfter)) return;
    console.error("swap: memories collapse detected, auto-restoring", {
      pass: guard.pass,
      snapshot: guard.snapshotId,
      before: guard.memoriesBefore,
      after: memoriesAfter
    });
    await restoreSwapSnapshot(env.DB, guard.snapshotId);
  } catch (error) {
    console.error("swap collapse check/restore failed", { pass: guard.pass, error: String(error) });
  }
}

function getDailyDigestNamespace(env: Env): string {
  return env.DREAM_NAMESPACE?.trim() || "default";
}

function getDailyDigestMaxRuns(env: Env): number {
  const parsed = Number(env.DREAM_MAX_RUNS || env.DAILY_DIGEST_MAX_RUNS || 10);
  if (!Number.isFinite(parsed)) return 10;
  return Math.min(Math.max(Math.floor(parsed), 1), 10);
}

async function runDailyMemoryDigestBatches(env: Env, namespace: string): Promise<unknown[]> {
  const results: unknown[] = [];
  const maxRuns = getDailyDigestMaxRuns(env);

  for (let i = 0; i < maxRuns; i += 1) {
    const result = await runDailyMemoryDigest(env, namespace, { trigger: "cron" });
    results.push({ type: "primary", result });
    if (!result.ran || !result.stats?.hasMore) break;
  }

  const backfill = await runDreamBackfill(env, namespace, { maxDates: 2, lookback: 3 });
  for (const item of backfill) {
    results.push({ type: "backfill", date_label: item.dateLabel, result: item.result });
  }

  return results;
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === "GET" && (url.pathname === "/admin" || url.pathname === "/memory-admin")) {
      return handleAdmin();
    }

    if (request.method === "GET" && url.pathname === "/admin/starmap") {
      return handleStarmap();
    }

    if (request.method === "POST" && url.pathname === "/admin/weekly-rollup") {
      return handleWeeklyRollupAdmin(request, env);
    }

    if (request.method === "POST" && url.pathname === "/admin/weekly-approve") {
      return handleWeeklyApproveAdmin(request, env);
    }

    if (request.method === "POST" && url.pathname === "/admin/monthly-rollup") {
      return handleMonthlyRollupAdmin(request, env);
    }

    if (request.method === "GET" && url.pathname === "/admin/swap/list") {
      return handleSwapListAdmin(request, env);
    }

    if (request.method === "POST" && url.pathname === "/admin/swap/restore") {
      return handleSwapRestoreAdmin(request, env);
    }

    if (request.method === "POST" && url.pathname === "/admin/swap/drop") {
      return handleSwapDropAdmin(request, env);
    }

    if (request.method === "GET" && url.pathname === "/admin/diary") {
      return handleDiaryAdmin(request, env);
    }

    if (request.method === "POST" && url.pathname === "/admin/diary-rewrite") {
      return handleDiaryRewriteAdmin(request, env);
    }

    if (request.method === "GET" && url.pathname === "/health") {
      return handleHealth(env);
    }

    if (request.method === "GET" && url.pathname === "/v1/models") {
      return handleModels(request, env);
    }

    if (request.method === "POST" && url.pathname === "/v1/chat/completions") {
      return handleChatCompletions(request, env, ctx);
    }

    if (
      request.method === "POST" &&
      (url.pathname === "/v1/guide-dog/chat/completions" || url.pathname === "/guide-dog/v1/chat/completions")
    ) {
      return handleGuideDogChatCompletions(request, env);
    }

    if (url.pathname === "/mcp" || url.pathname === "/memory-mcp") {
      return handleMcp(request, env, ctx);
    }

    if (url.pathname.startsWith("/v1/memories")) {
      return handleMemories(request, env, ctx);
    }

    if (url.pathname === "/api/memories/export") {
      return handleMemories(request, env, ctx);
    }

    if (request.method === "GET" && url.pathname === "/api/relations/graph") {
      return handleRelationsGraph(request, env);
    }

    if (url.pathname === "/v1/memory" || url.pathname.startsWith("/v1/memory/")) {
      return handleMemories(request, env, ctx);
    }

    if (url.pathname === "/v1/memory_boot") {
      return handleMemoryBoot(request, env);
    }

    if (url.pathname === "/v1/diary" || url.pathname === "/v1/diary/recent") {
      return handleDiaryApi(request, env);
    }

    if (url.pathname === "/v1/precious" || url.pathname.startsWith("/v1/precious/")) {
      return handlePrecious(request, env);
    }

    if (url.pathname === "/v1/glossary" || url.pathname.startsWith("/v1/glossary/")) {
      return handleGlossaryApi(request, env);
    }

    if (url.pathname.startsWith("/v1/longtail/")) {
      return handleLongtailApi(request, env);
    }

    if (url.pathname === "/v1/candidates" || url.pathname.startsWith("/v1/candidates/")) {
      return handleMemoryCandidates(request, env);
    }

    if (
      request.method === "POST" &&
      (url.pathname === "/v1/ingest/messages" || url.pathname === "/v1/messages/ingest")
    ) {
      return handleIngestMessagesApi(request, env, ctx);
    }

    if (request.method === "POST" && url.pathname === "/v1/search/memories") {
      return handleSearchMemoriesApi(request, env);
    }

    if (url.pathname.startsWith("/v1/cache/")) {
      return handleCache(request, env);
    }

    if (request.method === "GET" && url.pathname === "/v1/debug/cache_health") {
      return handleCacheHealth(request, env);
    }

    if (request.method === "GET" && url.pathname === "/v1/debug/vector_health") {
      return handleVectorHealth(request, env);
    }

    if (request.method === "POST" && url.pathname === "/v1/debug/vector_reindex") {
      return handleVectorReindex(request, env);
    }

    if (request.method === "POST" && url.pathname === "/v1/vector-doctor") {
      return handleVectorDoctor(request, env);
    }

    if (request.method === "GET" && url.pathname === "/admin/dream/harvest") {
      return handleDreamHarvest(request, env);
    }

    if (request.method === "GET" && url.pathname === "/v1/dream/status") {
      return handleDreamStatus(request, env);
    }

    if (request.method === "POST" && url.pathname === "/v1/dream/run") {
      return handleDreamRun(request, env);
    }

    return openAiError("Not found", 404);
  },

  async queue(batch: MessageBatch<QueueMessage>, env: Env): Promise<void> {
    for (const message of batch.messages) {
      try {
        await handleQueueMessage(message.body, env);
        message.ack();
      } catch (error) {
        console.error("queue message failed", error);
        message.retry();
      }
    }
  },

  async scheduled(controller: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
    const namespace = getDailyDigestNamespace(env);
    const cron = controller.cron;
    const shouldRunDailyMaintenance = !cron || cron === DAILY_MAINTENANCE_CRON;

    if (!shouldRunDailyMaintenance) {
      console.log("scheduled memory maintenance skipped unknown cron", { namespace, cron });
      return;
    }

    ctx.waitUntil(
      (async () => {
        const results: unknown[] = [];

        const dreamGuard = await takeSwapGuard(env, "dream", SWAP_DREAM_TABLES);
        const dreamResults = await runDailyMemoryDigestBatches(env, namespace);
        results.push({ type: "dream_batches", results: dreamResults });
        await checkSwapCollapse(env, dreamGuard);

        // Rollup phase triggers (diary → github∥retention → weekly → monthly). Order preserved.
        let diaryWriter: Awaited<ReturnType<typeof runDiaryTrigger>> | undefined;
        try {
          diaryWriter = await runDiaryTrigger(env, namespace);
        } catch (error) {
          console.error("scheduled diary writer failed", {
            namespace,
            error: error instanceof Error ? error.message : String(error)
          });
        }
        results.push({ type: "diary_writer", result: diaryWriter ?? { ok: false } });

        const retentionGuard = await takeSwapGuard(env, "retention", SWAP_RETENTION_TABLES);
        const [retentionResult, githubResult] = await Promise.all([
          runMemoryRetention(env, namespace).then(
            (retention) => ({ ok: true as const, retention }),
            (error) => {
              console.error("scheduled memory retention failed", { namespace, error: String(error) });
              return { ok: false as const, error: String(error) };
            }
          ),
          // 04:10 SGT cron (20:10 UTC) runs ~4h after cmh-lite's 23:50 local push — safe to pull yesterday's daily.
          runGithubDailyTrigger(env).then(
            (r) => {
              console.log("github daily pull", r);
              return r;
            },
            (e) => {
              console.error("github daily pull failed", String(e));
              return { ok: false };
            }
          )
        ]);
        results.push({ type: "retention", result: retentionResult });
        results.push({ type: "github_daily", result: githubResult });
        // 只在 retention 真跑过 (非 24h 节流跳过) 时才比数。
        if (retentionResult.ok && retentionResult.retention.ran) {
          await checkSwapCollapse(env, retentionGuard);
        }

        let weeklyRollup: Awaited<ReturnType<typeof runWeeklyRollupTrigger>> | undefined;
        try {
          weeklyRollup = await runWeeklyRollupTrigger(env, namespace);
        } catch (error) {
          console.error("scheduled weekly rollup failed", {
            namespace,
            error: error instanceof Error ? error.message : String(error)
          });
        }
        results.push({ type: "weekly_rollup", result: weeklyRollup ?? { ok: false } });

        let monthlyRollup: Awaited<ReturnType<typeof runMonthlyRollupTrigger>> | undefined;
        // 月记会删 weekly_log：先拍快照留退路，不做崩坍比对 (weekly_log 本来就该缩)。
        await takeSwapGuard(env, "monthly", SWAP_MONTHLY_TABLES);
        try {
          monthlyRollup = await runMonthlyRollupTrigger(env, namespace);
        } catch (error) {
          console.error("scheduled monthly rollup failed", {
            namespace,
            error: error instanceof Error ? error.message : String(error)
          });
        }
        results.push({ type: "monthly_rollup", result: monthlyRollup ?? { ok: false } });

        let swapCleanup: Awaited<ReturnType<typeof cleanupSwapSnapshots>> | undefined;
        try {
          if (isSwapEnabled(env)) swapCleanup = await cleanupSwapSnapshots(env.DB);
        } catch (error) {
          console.error("scheduled swap cleanup failed", {
            error: error instanceof Error ? error.message : String(error)
          });
        }
        if (swapCleanup) results.push({ type: "swap_cleanup", result: swapCleanup });

        console.log("scheduled memory maintenance", { namespace, cron, results });
      })()
    );
  }
};