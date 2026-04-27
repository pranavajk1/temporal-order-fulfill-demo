import type { ExecuteQueryInput, ExecuteQueryResult } from './execute-query';

/**
 * Activity #2 (dispatchActions) — PODE-3419 stand-in: read a “result” row set, dispatch
 * per row, checkpoint progress in activity heartbeats, resume on retry from heartbeatDetails.
 */
export type DispatchDataMode = 'memory' | 'seed' | 'inventory';

export interface DispatchProgressHeartbeat {
  /** run correlation (logical “result” name) */
  resultTableName: string;
  runId: string;
  totalDispatchedThisRun: number;
  /** 'memory' is used when DATABASE_URL is unset. */
  source: 'pg' | 'memory';
  mode: DispatchDataMode;
  /**
   * Seed `pode_result_rows`: first 0-based ord not yet dispatched.
   * Ignored in inventory mode.
   */
  nextRowOrd?: number;
  /**
   * Inventory / keyset mode: all rows with `id` <= this value have been processed for this run.
   * Next read uses `WHERE "id" > $lastId` (string keeps bigint lossless in JSON).
   */
  lastIdProcessed?: string;
}

export interface InventoryTableRef {
  /** e.g. `public` */
  schema?: string;
  /** e.g. `inventory_items` */
  table: string;
  /** Keyset column, usually `id` (must be orderable, typically bigint / int). */
  idColumn?: string;
}

export interface DispatchActionsInput {
  resultTableName: string;
  runId: string;
  /**
   * Seed / memory only: how many source rows to ensure or synthesize. Ignored when
   * `useInventoryTable` is true.
   */
  rowCount?: number;
  /**
   * `false`: synthetic `pode_result_rows` demo. When omitted and the worker has `DATABASE_URL`, the
   * activity defaults to **true** and scans the real `public.inventory_items` (or `PODE_DISPATCH_TABLE`).
   * Set env `PODE_DISPATCH_USE_SEED=1` on the worker to use the small demo without passing `false` here.
   */
  useInventoryTable?: boolean;
  /**
   * Physical table to scan (default from env, see `resolveInventoryTable()` in dispatch-actions).
   */
  inventory?: InventoryTableRef;
  /**
   * Hard cap for how many rows to process in this run (0 = no cap, scan to EOF).
   * For large DBs, set e.g. 10_000 while developing.
   */
  maxRowsToDispatch?: number;
  /**
   * Rows to fetch per SELECT. Larger = fewer round-trips (default 2000 for inventory).
   */
  batchSize?: number;
  /** How many source rows to process per activity.heartbeat (checkpoint granularity). */
  rowsPerHeartbeat?: number;
  /** Simulated per-row I/O. Use 0 for real large scans. */
  perRowSimulatedMs?: number;
  /**
   * Seed / memory: throw after processing a row with this 0-based `ord`.
   * Omitted in inventory mode (avoids conflating with id gaps).
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
  /** Inventory: last id processed, if any. */
  lastIdProcessed?: string;
  source: 'pg' | 'memory';
  mode: DispatchDataMode;
  sourceTable?: string;
}

/**
 * `executeQuery` then `dispatchActions`; default `rowCount` for seed/memo comes from the
 * query result. Ignored for inventory (use `maxRowsToDispatch` in `dispatchOptions` instead).
 */
export interface PodeQueryPipelineInput extends ExecuteQueryInput {
  dispatchOptions?: Pick<
    DispatchActionsInput,
    | 'rowsPerHeartbeat'
    | 'perRowSimulatedMs'
    | 'failOnRowIndex'
    | 'preferInMemorySource'
    | 'rowCount'
    | 'useInventoryTable'
    | 'inventory'
    | 'maxRowsToDispatch'
    | 'batchSize'
  >;
}

export interface PodeQueryPipelineResult {
  query: ExecuteQueryResult;
  dispatch: DispatchActionsResult;
}
