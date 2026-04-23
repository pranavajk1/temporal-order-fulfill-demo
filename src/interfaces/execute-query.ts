/**
 * Activity #1 (executeQuery) — wire format for PODE-3419.
 * Trino is optional; the spike can simulate wall-clock work with heartbeats.
 */
export interface ExecuteQueryInput {
  partnerId: string;
  queryId: string;
  runId: string;
  /** Total simulated “query” time in ms. Default 60s. */
  simulatedDurationMs?: number;
  /** How often to call activity.heartbeat (must stay under heartbeatTimeout). Default 5s. */
  heartbeatIntervalMs?: number;
}

export interface ExecuteQueryResult {
  /** `<partner_id>_<query_id>_<run_id>_result` */
  resultTableName: string;
  /** Placeholder until a real engine reports row count */
  simulatedRowCount: number;
}
