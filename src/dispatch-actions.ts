import * as activity from '@temporalio/activity';
import type { Pool, PoolConfig } from 'pg';
import type {
  DispatchActionsInput,
  DispatchActionsResult,
  DispatchProgressHeartbeat,
} from './interfaces/dispatch-actions';

const DEFAULT_ROW_COUNT = 100;
const DEFAULT_ROWS_PER_HB = 20;
const DEFAULT_PER_ROW_MS = 2;
const MAX_ROWS_PER_READ = 500;

/** In-process store when DATABASE_URL is not used. */
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

function parseStartOrd(details: unknown, resultTableName: string, runId: string): { start: number; resumed: boolean } {
  if (!details || typeof details !== 'object') {
    return { start: 0, resumed: false };
  }
  const d = details as Partial<DispatchProgressHeartbeat>;
  if (d.resultTableName === resultTableName && d.runId === runId && typeof d.nextRowOrd === 'number' && d.nextRowOrd >= 0) {
    return { start: d.nextRowOrd, resumed: true };
  }
  return { start: 0, resumed: false };
}

function shouldUsePostgres(explicit: boolean | undefined, hasUrl: boolean): boolean {
  if (explicit === true) {
    return false;
  }
  if (explicit === false) {
    return hasUrl;
  }
  return hasUrl;
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

async function ensureRowsPg(
  pool: Pool,
  sourceKey: string,
  rowCount: number
): Promise<void> {
  const { rows } = await pool.query<{ c: string }>(`SELECT COUNT(*)::int AS c FROM pode_result_rows WHERE source_key = $1`, [sourceKey]);
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
  let block = memoryBySource.get(sourceKey);
  if (!block || block.length < rowCount) {
    block = Array.from({ length: rowCount }, (_, o) => ({
      ord: o,
      data: `row ${o} for ${sourceKey}`,
    }));
    memoryBySource.set(sourceKey, block);
  }
}

/**
 * PODE-3419 activity #2: “dispatch” one row at a time with periodic heartbeats.
 * On retry, reads {@link import('@temporalio/activity').Context.current().info.heartbeatDetails}
 * and resumes at `nextRowOrd`.
 */
export async function dispatchActions(input: DispatchActionsInput): Promise<DispatchActionsResult> {
  const {
    resultTableName,
    runId,
    rowCount = DEFAULT_ROW_COUNT,
    rowsPerHeartbeat = DEFAULT_ROWS_PER_HB,
    perRowSimulatedMs = DEFAULT_PER_ROW_MS,
    failOnRowIndex,
    preferInMemorySource,
  } = input;

  const hasUrl = Boolean(process.env.DATABASE_URL);
  const usePg = shouldUsePostgres(preferInMemorySource, hasUrl);

  if (!usePg) {
    activity.log.info('dispatchActions: using in-memory result source (set DATABASE_URL to use Postgres)');
  }

  const { heartbeatDetails } = activity.Context.current().info;
  const { start, resumed } = parseStartOrd(heartbeatDetails, resultTableName, runId);

  if (resumed) {
    activity.log.info('dispatchActions: resuming from heartbeat checkpoint', { startRowOrd: start });
  }

  if (usePg) {
    const pool = await getPool();
    await ensureTablePg(pool);
    await ensureRowsPg(pool, resultTableName, rowCount);
  } else {
    ensureRowsMemory(resultTableName, rowCount);
  }

  const source: 'pg' | 'memory' = usePg ? 'pg' : 'memory';
  let nextRowOrd = start;
  let total = 0;
  const hbEvery = Math.max(1, rowsPerHeartbeat);

  function sendHeartbeat(doneOrdExclusive: number): void {
    const hb: DispatchProgressHeartbeat = {
      nextRowOrd: doneOrdExclusive,
      resultTableName,
      runId,
      totalDispatchedThisRun: total,
      source,
    };
    activity.heartbeat(hb);
  }

  while (nextRowOrd < rowCount) {
    const batchLimit = Math.min(MAX_ROWS_PER_READ, rowCount - nextRowOrd);
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
      if (usePg) {
        await (await getPool()).query(
          `UPDATE pode_result_rows SET status = 'dispatched' WHERE source_key = $1 AND ord = $2`,
          [resultTableName, rec.ord]
        );
      }
      total += 1;
      sinceLastHb += 1;
      const doneOrdExclusive = rec.ord + 1;
      if (sinceLastHb >= hbEvery || doneOrdExclusive >= rowCount) {
        sendHeartbeat(doneOrdExclusive);
        sinceLastHb = 0;
      }
      nextRowOrd = doneOrdExclusive;
      if (failOnRowIndex != null && rec.ord === failOnRowIndex) {
        if (sinceLastHb > 0) {
          sendHeartbeat(doneOrdExclusive);
        }
        throw new Error(
          `Simulated dispatch failure after row ord=${rec.ord} (set failOnRowIndex) — next attempt resumes at nextRowOrd=${doneOrdExclusive}`
        );
      }
    }
  }

  const result: DispatchActionsResult = {
    dispatched: total,
    resultTableName,
    runId,
    resumed,
    startRowOrd: start,
    source,
  };
  activity.heartbeat({ ...result, done: true as const });
  return result;
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
