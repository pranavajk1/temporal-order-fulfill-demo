import * as activity from '@temporalio/activity';
import type { ExecuteQueryInput, ExecuteQueryResult } from './interfaces/execute-query';

/**
 * Stand-in for a long Trino (or any) query: sleeps for simulatedDurationMs and
 * heartbeats on a fixed interval so the server knows the worker is alive and
 * UIs / DescribeWorkflowExecution can show progress.
 *
 * Temporal behavior this exercises:
 * - startToCloseTimeout on the activity can be large (hours).
 * - heartbeatTimeout should be short; if the process dies, the activity fails
 *   after that window without a heartbeat, instead of waiting for start-to-close.
 * - Heartbeat payloads are recorded on the activity (visible in UI / history).
 */
export async function simulateLongQuery(input: ExecuteQueryInput): Promise<ExecuteQueryResult> {
  const {
    partnerId,
    queryId,
    runId,
    simulatedDurationMs = 60_000,
    heartbeatIntervalMs = 5_000,
  } = input;

  const resultTableName = `${partnerId}_${queryId}_${runId}_result`;
  const ctx = activity.Context.current();
  const startedAt = Date.now();
  const endAt = startedAt + simulatedDurationMs;

  let i = 0;
  while (Date.now() < endAt) {
    ctx.cancellationSignal.throwIfAborted();
    const now = Date.now();
    const elapsedMs = now - startedAt;
    const percentDone = Math.min(99, Math.round((elapsedMs / simulatedDurationMs) * 100));
    activity.heartbeat({
      phase: 'running' as const,
      tick: i,
      percentDone,
      resultTableName,
      elapsedMs,
    });
    i += 1;
    const remaining = endAt - now;
    if (remaining <= 0) {
      break;
    }
    await sleep(Math.min(heartbeatIntervalMs, remaining));
  }

  activity.heartbeat({
    phase: 'complete' as const,
    percentDone: 100,
    resultTableName,
    elapsedMs: Math.min(simulatedDurationMs, Date.now() - startedAt),
  });

  const simulatedRowCount = Math.max(0, Math.floor(simulatedDurationMs / 10));

  return { resultTableName, simulatedRowCount };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
