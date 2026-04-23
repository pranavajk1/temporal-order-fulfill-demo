import type { ExecuteQueryInput, ExecuteQueryResult } from './execute-query';

/**
 * Activity #2 (dispatchActions) — PODE-3419 stand-in: read a “result” row set, dispatch
 * per row, checkpoint progress in activity heartbeats, resume on retry from heartbeatDetails.
 */
export interface DispatchProgressHeartbeat {
  /** First row not yet fully dispatched (0-based, matches `ord` in source) */
  nextRowOrd: number;
  resultTableName: string;
  runId: string;
  totalDispatchedThisRun: number;
  /** 'memory' is used when DATABASE_URL is unset; set POStgres URL to exercise the DB path. */
  source: 'pg' | 'memory';
}

export interface DispatchActionsInput {
  resultTableName: string;
  runId: string;
  /** How many source rows to ensure exist (seeding). Defaults from a small demo default if omitted. */
  rowCount?: number;
  /** How many source rows to process per activity.heartbeat (liveness + checkpoint). */
  rowsPerHeartbeat?: number;
  /** Simulated per-row work (I/O) so heartbeats are meaningful. */
  perRowSimulatedMs?: number;
  /**
   * When this equals the `ord` of a row, throw after that row is processed (and after heartbeat
   * for that step) to force a retry. Omit for no forced failure. Used only for resume demo.
   */
  failOnRowIndex?: number;
  /**
   * When true, use in-memory source even if DATABASE_URL is set (tests).
   * When unset and DATABASE_URL is set, use Postgres. Otherwise in-memory.
   */
  preferInMemorySource?: boolean;
}

export interface DispatchActionsResult {
  dispatched: number;
  resultTableName: string;
  runId: string;
  resumed: boolean;
  startRowOrd: number;
  source: 'pg' | 'memory';
}

/**
 * `executeQuery` then `dispatchActions`; default row count comes from the query result
 * (unless overridden in `dispatchOptions.rowCount`).
 */
export interface PodeQueryPipelineInput extends ExecuteQueryInput {
  dispatchOptions?: Pick<
    DispatchActionsInput,
    'rowsPerHeartbeat' | 'perRowSimulatedMs' | 'failOnRowIndex' | 'preferInMemorySource' | 'rowCount'
  >;
}

export interface PodeQueryPipelineResult {
  query: ExecuteQueryResult;
  dispatch: DispatchActionsResult;
}
