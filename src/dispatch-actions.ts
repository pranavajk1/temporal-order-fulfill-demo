import * as activity from '@temporalio/activity';
import type { Pool, PoolConfig } from 'pg';
import type {
  DispatchActionsInput,
  DispatchActionsResult,
  DispatchDataMode,
  DispatchProgressHeartbeat,
} from './interfaces/dispatch-actions';

const DEFAULT_ROW_COUNT = 100;
const DEFAULT_ROWS_PER_HB = 20;
const DEFAULT_PER_ROW_MS = 2;
const DEFAULT_SEED_READ_BATCH = 500;
const DEFAULT_INVENTORY_READ_BATCH = 2000;
const DEFAULT_INVENTORY_ROWS_PER_HB = 2000;

const memoryBySource = new Map<string, Array<{ ord: number; data: string }>>();

let pgPool: Pool | null = null;
async function getPool(): Promise<Pool> {
  if (pgPool) {
    return pgPool;
  }
  const { Pool: PgPool } = await import('pg');
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('getPool: DATABASE_URL expected');
  }
  const config: PoolConfig = { connectionString, max: 4 };
  pgPool = new PgPool(config);
  return pgPool;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Set `1` to use the tiny synthetic `pode_result_rows` demo (32 rows) instead of `public.inventory_items`. */
function envWantsSmallSeedTable(): boolean {
  const v = process.env.PODE_DISPATCH_USE_SEED;
  return v === '1' || v === 'true' || v === 'yes';
}

function maxRowsFromEnv(): number {
  const r = process.env.PODE_DISPATCH_MAX_ROWS;
  if (r == null || r === '') {
    return 0;
  }
  const n = parseInt(r, 10);
  return Number.isFinite(n) && n >= 0 ? n : 0;
}

/**
 * Resolves which physical table to scan. Env can override; input.inventory wins.
 */
function resolveInventoryTable(input: DispatchActionsInput): { schema: string; table: string; idColumn: string } {
  const inv = input.inventory;
  const r = {
    schema: inv?.schema ?? process.env.PODE_DISPATCH_SCHEMA ?? 'public',
    table: inv?.table ?? process.env.PODE_DISPATCH_TABLE ?? 'inventory_items',
    idColumn: inv?.idColumn ?? process.env.PODE_DISPATCH_ID_COLUMN ?? 'id',
  };
  assertSafeSqlIdent(r.schema, 'schema');
  assertSafeSqlIdent(r.table, 'table');
  assertSafeSqlIdent(r.idColumn, 'idColumn');
  return r;
}

function assertSafeSqlIdent(name: string, what: string): void {
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
    throw new Error(`dispatch (inventory): invalid ${what} ${JSON.stringify(name)} (use simple identifiers only)`);
  }
}

function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

function shouldUsePostgresInMemoryPath(explicit: boolean | undefined, hasUrl: boolean): boolean {
  if (explicit === true) {
    return false;
  }
  if (explicit === false) {
    return hasUrl;
  }
  return hasUrl;
}

/**
 * With `DATABASE_URL` on the **worker** (e.g. project `.env` + `import 'dotenv/config'`), the default
 * is to scan the real `inventory_items` (or `PODE_DISPATCH_TABLE`). Opt out: `PODE_DISPATCH_USE_SEED=1` or
 * `useInventoryTable: false` in the workflow input.
 */
function useInventoryEnabled(input: DispatchActionsInput, hasUrl: boolean): boolean {
  if (input.preferInMemorySource) {
    return false;
  }
  if (!hasUrl) {
    return false;
  }
  if (input.useInventoryTable === false) {
    return false;
  }
  if (envWantsSmallSeedTable()) {
    return false;
  }
  return true;
}

function parseStartSeed(
  details: unknown,
  resultTableName: string,
  runId: string
): { start: number; resumed: boolean } {
  if (!details || typeof details !== 'object') {
    return { start: 0, resumed: false };
  }
  const d = details as Partial<DispatchProgressHeartbeat>;
  if (d.mode === 'inventory' || d.lastIdProcessed != null) {
    return { start: 0, resumed: false };
  }
  if (d.resultTableName === resultTableName && d.runId === runId && typeof d.nextRowOrd === 'number' && d.nextRowOrd >= 0) {
    return { start: d.nextRowOrd, resumed: true };
  }
  return { start: 0, resumed: false };
}

function parseStartInventory(
  details: unknown,
  resultTableName: string,
  runId: string
): { lastId: string; resumed: boolean } {
  if (!details || typeof details !== 'object') {
    return { lastId: '0', resumed: false };
  }
  const d = details as Partial<DispatchProgressHeartbeat>;
  if (d.resultTableName !== resultTableName || d.runId !== runId) {
    return { lastId: '0', resumed: false };
  }
  /* Don’t treat seed (ordinal) checkpoints as a keyset cursor. */
  if (d.mode === 'seed' || (d.nextRowOrd != null && d.lastIdProcessed == null)) {
    return { lastId: '0', resumed: false };
  }
  if (d.lastIdProcessed == null) {
    return { lastId: '0', resumed: false };
  }
  if (d.lastIdProcessed === '') {
    return { lastId: '0', resumed: true };
  }
  return { lastId: d.lastIdProcessed, resumed: true };
}

function rowIdToString(id: unknown): string {
  if (typeof id === 'bigint' || typeof id === 'number') {
    return id.toString();
  }
  return String(id);
}

async function ensureTablePg(pool: Pool): Promise<void> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS pode_result_rows (
      id BIGSERIAL PRIMARY KEY,
      source_key TEXT NOT NULL,
      ord INT NOT NULL,
      data TEXT,
      status TEXT NOT NULL DEFAULT 'pending',
      UNIQUE (source_key, ord)
    )
  `);
}

async function ensureRowsPg(pool: Pool, sourceKey: string, rowCount: number): Promise<void> {
  const { rows } = await pool.query<{ c: string }>(
    `SELECT COUNT(*)::int AS c FROM pode_result_rows WHERE source_key = $1`,
    [sourceKey]
  );
  const n = parseInt(rows[0].c, 10);
  if (n >= rowCount) {
    return;
  }
  for (let o = n; o < rowCount; o += 1) {
    await pool.query(
      `INSERT INTO pode_result_rows (source_key, ord, data) VALUES ($1, $2, $3) ON CONFLICT (source_key, ord) DO NOTHING`,
      [sourceKey, o, `row ${o} for ${sourceKey}`]
    );
  }
}

function ensureRowsMemory(sourceKey: string, rowCount: number): void {
  if (!memoryBySource.get(sourceKey) || memoryBySource.get(sourceKey)!.length < rowCount) {
    const block = Array.from({ length: rowCount }, (_, o) => ({
      ord: o,
      data: `row ${o} for ${sourceKey}`,
    }));
    memoryBySource.set(sourceKey, block);
  }
}

async function fetchBatchPg(
  sourceKey: string,
  fromOrd: number,
  limit: number,
  pool: Pool
): Promise<Array<{ ord: number; data: string }>> {
  const { rows } = await pool.query<{ ord: number; data: string }>(
    `SELECT ord, data FROM pode_result_rows
     WHERE source_key = $1 AND ord >= $2
     ORDER BY ord ASC
     LIMIT $3`,
    [sourceKey, fromOrd, limit]
  );
  return rows;
}

/**
 * Inventory path: no OFFSET; keyset on `id` for large tables.
 * Does not modify `inventory_items` (read-only “dispatch” simulation).
 */
async function dispatchFromInventory(
  input: DispatchActionsInput,
  pool: Pool
): Promise<DispatchActionsResult> {
  const {
    resultTableName,
    runId,
    rowsPerHeartbeat = DEFAULT_INVENTORY_ROWS_PER_HB,
    perRowSimulatedMs = 0,
  } = input;
  const batchSize = input.batchSize ?? DEFAULT_INVENTORY_READ_BATCH;
  const fromInput = input.maxRowsToDispatch;
  const fromEnv = maxRowsFromEnv();
  const effectiveLimit =
    fromInput != null && fromInput > 0
      ? fromInput
      : fromEnv > 0
        ? fromEnv
        : 0;
  const cap = effectiveLimit > 0 ? effectiveLimit : Number.POSITIVE_INFINITY;
  const ref = resolveInventoryTable(input);
  const fullTable = `${ref.schema}.${ref.table}`;

  const { heartbeatDetails } = activity.Context.current().info;
  const { lastId, resumed } = parseStartInventory(heartbeatDetails, resultTableName, runId);
  if (resumed) {
    activity.log.info('dispatchActions (inventory): resuming from heartbeat checkpoint', {
      lastId,
      table: fullTable,
    });
  } else {
    activity.log.info('dispatchActions (inventory): scanning with keyset pagination', {
      table: fullTable,
      idColumn: ref.idColumn,
      maxRowsToDispatch: cap === Number.POSITIVE_INFINITY ? 'unlimited' : cap,
      batchSize,
    });
  }

  let lastIdCursor = lastId;
  let total = 0;
  const hbEvery = Math.max(1, rowsPerHeartbeat);
  let sinceLastHb = 0;
  const mode: DispatchDataMode = 'inventory';
  const source: 'pg' = 'pg';

  function sendHb(last: string, totalDisp: number): void {
    const hb: DispatchProgressHeartbeat = {
      mode: 'inventory',
      lastIdProcessed: last,
      resultTableName,
      runId,
      totalDispatchedThisRun: totalDisp,
      source,
    };
    activity.heartbeat(hb);
  }

  for (;;) {
    if (total >= cap) {
      break;
    }
    const remaining = cap - total;
    const thisLimit = Math.min(batchSize, Math.min(50000, Math.floor(remaining)));
    const poolRows = await fetchInventoryKeyset(pool, ref, lastIdCursor, thisLimit);
    if (poolRows.length === 0) {
      break;
    }

    for (const r of poolRows) {
      activity.Context.current().cancellationSignal.throwIfAborted();
      await performPerRowDispatchActionInventory(perRowSimulatedMs, r);
      total += 1;
      sinceLastHb += 1;
      const idStr = rowIdToString(r.id);
      lastIdCursor = idStr;

      if (sinceLastHb >= hbEvery || total >= cap) {
        sendHb(idStr, total);
        sinceLastHb = 0;
      }
      if (total >= cap) {
        if (sinceLastHb > 0) {
          sendHb(idStr, total);
        }
        break;
      }
    }
    if (total >= cap) {
      break;
    }
  }

  const out: DispatchActionsResult = {
    dispatched: total,
    resultTableName,
    runId,
    resumed,
    startRowOrd: 0,
    lastIdProcessed: lastIdCursor,
    source,
    mode,
    sourceTable: fullTable,
  };
  activity.heartbeat({ ...out, done: true as const });
  return out;
}

function tableNotFoundHint(qualified: string, err: unknown): Error {
  const e = err as { code?: string; message?: string } | null;
  if (e && e.code === '42P01') {
    return new Error(
      `Postgres: relation ${qualified} does not exist. The worker did connect. ` +
        'Fix: (1) Same DATABASE_URL as the client you used in tests (see database name after the host:port, e.g. /plume). ' +
        '(2) If the table is not in `public`, set PODE_DISPATCH_SCHEMA. ' +
        '(3) If the name differs, set PODE_DISPATCH_TABLE. ' +
        'See .env.example. ' +
        `Original: ${e.message ?? String(err)}`
    );
  }
  return err instanceof Error ? err : new Error(String(err));
}

/**
 * One logical “dispatch” step: the work for a **single** result row (e.g. emit job, call downstream API).
 * Batches only size how many rows we read from Postgres per round trip; the unit of work is this call.
 * Inventory path is read-only: simulates latency only (no side effects on the scanned table by default).
 */
async function performPerRowDispatchActionInventory(
  perRowSimulatedMs: number,
  _row: { id: unknown }
): Promise<void> {
  if (perRowSimulatedMs > 0) {
    await sleep(perRowSimulatedMs);
  }
}

/** Seed / `pode_result_rows`: per-row “action” is a row status update (idempotent story for the demo). */
async function performPerRowDispatchActionSeed(
  usePg: boolean,
  resultTableName: string,
  rec: { ord: number; data: string },
  pool: Pool
): Promise<void> {
  if (usePg) {
    await pool.query(
      `UPDATE pode_result_rows SET status = 'dispatched' WHERE source_key = $1 AND ord = $2`,
      [resultTableName, rec.ord]
    );
  }
}

async function fetchInventoryKeyset(
  pool: Pool,
  ref: { schema: string; table: string; idColumn: string },
  lastIdExclusive: string,
  limit: number
): Promise<Array<{ id: unknown }>> {
  const t = `${quoteIdent(ref.schema)}.${quoteIdent(ref.table)}`;
  const col = quoteIdent(ref.idColumn);
  const q = `SELECT ${col} AS "id" FROM ${t} WHERE ${col} > $1 ORDER BY ${col} ASC LIMIT $2`;
  try {
    const { rows } = await pool.query(q, [lastIdExclusive, limit]);
    return rows as { id: unknown }[];
  } catch (e) {
    throw tableNotFoundHint(t, e);
  }
}

async function dispatchFromSeedOrMemory(
  input: DispatchActionsInput,
  dataMode: 'memory' | 'seed',
  hasUrl: boolean
): Promise<DispatchActionsResult> {
  const {
    resultTableName,
    runId,
    rowCount = DEFAULT_ROW_COUNT,
    rowsPerHeartbeat = DEFAULT_ROWS_PER_HB,
    perRowSimulatedMs = DEFAULT_PER_ROW_MS,
    failOnRowIndex,
  } = input;
  const usePg = dataMode === 'seed' && hasUrl;

  const { heartbeatDetails } = activity.Context.current().info;
  const { start, resumed } = parseStartSeed(heartbeatDetails, resultTableName, runId);

  if (resumed) {
    activity.log.info('dispatchActions: resuming from heartbeat checkpoint', { startRowOrd: start });
  }

  if (dataMode === 'seed' && usePg) {
    const pool = await getPool();
    await ensureTablePg(pool);
    await ensureRowsPg(pool, resultTableName, rowCount);
  } else {
    ensureRowsMemory(resultTableName, rowCount);
  }

  const source: 'pg' | 'memory' = usePg ? 'pg' : 'memory';
  const mode: DispatchDataMode = dataMode === 'memory' || !usePg ? 'memory' : 'seed';
  let nextRowOrd = start;
  let total = 0;
  const hbEvery = Math.max(1, rowsPerHeartbeat);
  const readLimit = dataMode === 'memory' ? DEFAULT_SEED_READ_BATCH : DEFAULT_SEED_READ_BATCH;

  function sendSeedHb(doneOrdExclusive: number, totalDisp: number): void {
    const hb: DispatchProgressHeartbeat = {
      mode: 'seed',
      nextRowOrd: doneOrdExclusive,
      resultTableName,
      runId,
      totalDispatchedThisRun: totalDisp,
      source: usePg ? 'pg' : 'memory',
    };
    activity.heartbeat(hb);
  }

  while (nextRowOrd < rowCount) {
    const batchLimit = Math.min(readLimit, rowCount - nextRowOrd);
    const ordRows = usePg
      ? await fetchBatchPg(resultTableName, nextRowOrd, batchLimit, await getPool())
      : (memoryBySource.get(resultTableName) ?? []).slice(nextRowOrd, nextRowOrd + batchLimit);

    if (ordRows.length === 0) {
      break;
    }

    let sinceLastHb = 0;
    for (const rec of ordRows) {
      activity.Context.current().cancellationSignal.throwIfAborted();
      if (perRowSimulatedMs > 0) {
        await sleep(perRowSimulatedMs);
      }
      await performPerRowDispatchActionSeed(usePg, resultTableName, rec, await getPool());
      total += 1;
      sinceLastHb += 1;
      const doneOrdExclusive = rec.ord + 1;
      if (sinceLastHb >= hbEvery || doneOrdExclusive >= rowCount) {
        sendSeedHb(doneOrdExclusive, total);
        sinceLastHb = 0;
      }
      nextRowOrd = doneOrdExclusive;
      if (failOnRowIndex != null && rec.ord === failOnRowIndex) {
        if (sinceLastHb > 0) {
          sendSeedHb(doneOrdExclusive, total);
        }
        throw new Error(
          `Simulated dispatch failure after row ord=${rec.ord} (set failOnRowIndex) — next attempt resumes at nextRowOrd=${doneOrdExclusive}`
        );
      }
    }
  }

  const out: DispatchActionsResult = {
    dispatched: total,
    resultTableName,
    runId,
    resumed,
    startRowOrd: start,
    source,
    mode,
  };
  activity.heartbeat({ ...out, done: true as const });
  return out;
}

/**
 * PODE-3419 activity #2: dispatch with heartbeat checkpoints. Inventory mode uses keyset scans.
 */
export async function dispatchActions(input: DispatchActionsInput): Promise<DispatchActionsResult> {
  const hasUrl = Boolean(process.env.DATABASE_URL);
  if (input.useInventoryTable === true && !hasUrl) {
    throw new Error(
      'dispatchActions: useInventoryTable was requested (real table scan) but DATABASE_URL is missing in the worker ' +
        'process. Add it to a project .env and restart the worker, or set PODE_DISPATCH_USE_SEED=1 for the tiny local demo.'
    );
  }
  const useInventory = useInventoryEnabled(input, hasUrl);
  const inMemory = !shouldUsePostgresInMemoryPath(input.preferInMemorySource, hasUrl);
  const path: 'memory' | 'seed_pg' | 'inventory' =
    inMemory && !useInventory ? 'memory' : useInventory ? 'inventory' : 'seed_pg';
  activity.log.info('dispatchActions: data path', {
    path,
    hasUrl,
    useInventoryTable: input.useInventoryTable,
    PODE_DISPATCH_USE_SEED: process.env.PODE_DISPATCH_USE_SEED,
  });

  if (inMemory && !useInventory) {
    activity.log.info('dispatchActions: in-memory (no DATABASE_URL or preferInMemorySource)');
    return dispatchFromSeedOrMemory(input, 'memory', hasUrl);
  }
  if (inMemory && useInventory) {
    throw new Error(
      'dispatchActions: useInventoryTable requires DATABASE_URL; unset preferInMemorySource to use Postgres'
    );
  }
  if (useInventory) {
    const pool = await getPool();
    return dispatchFromInventory(input, pool);
  }
  return dispatchFromSeedOrMemory(input, 'seed', hasUrl);
}
