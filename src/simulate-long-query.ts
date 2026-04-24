import * as activity from '@temporalio/activity';
import type { ExecuteQueryInput, ExecuteQueryResult } from './interfaces/execute-query';

/**
 * Sent on every `activity.heartbeat` call. The server persists the last payload;
 * on a new attempt after failure (e.g. worker bounce), `Context.current().info.heartbeatDetails`
 * is set to that last payload so the elapsed portion of the run can be skipped.
 */
export interface LongQueryHeartbeatPayload {
  phase: 'running' | 'complete';
  tick: number;
  percentDone: number;
  resultTableName: string;
  /** Logical elapsed time in this simulated “query” (0 … simulatedDurationMs) */
  elapsedMs: number;
  /** Set when this attempt is continuing from a previous attempt’s last heartbeat */
  resumedFromLastAttempt?: boolean;
}

function resumeFromDetails(
  details: unknown,
  resultTableName: string,
  simulatedDurationMs: number
): { alreadyElapsedMs: number; nextTick: number } {
  if (details == null || typeof details !== 'object') {
    return { alreadyElapsedMs: 0, nextTick: 0 };
  }
  const p = details as Partial<LongQueryHeartbeatPayload>;
  if (p.resultTableName !== resultTableName) {
    return { alreadyElapsedMs: 0, nextTick: 0 };
  }
  if (p.phase === 'complete' && typeof p.elapsedMs === 'number') {
    return {
      alreadyElapsedMs: Math.min(simulatedDurationMs, p.elapsedMs),
      nextTick: typeof p.tick === 'number' ? p.tick + 1 : 0,
    };
  }
  if (p.phase === 'running' && typeof p.elapsedMs === 'number' && typeof p.tick === 'number') {
    return {
      alreadyElapsedMs: Math.max(0, Math.min(p.elapsedMs, simulatedDurationMs)),
      nextTick: p.tick + 1,
    };
  }
  return { alreadyElapsedMs: 0, nextTick: 0 };
}


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
  const { alreadyElapsedMs, nextTick } = resumeFromDetails(
    ctx.info.heartbeatDetails,
    resultTableName,
    simulatedDurationMs
  );
  if (ctx.info.attempt > 1 && (alreadyElapsedMs > 0 || nextTick > 0)) {
    console.log(
      `[simulateLongQuery] attempt ${ctx.info.attempt}: resuming from heartbeatDetails ` +
        `(~${alreadyElapsedMs}ms elapsed, next tick ${nextTick})`
    );
  }
  // Virtual start so (Date.now() - startedAt) == logical elapsed including prior attempt
  const startedAt = Date.now() - alreadyElapsedMs;
  const endAt = startedAt + simulatedDurationMs;

  let i = nextTick;
  if (Date.now() >= endAt) {
    activity.heartbeat({
      phase: 'complete',
      percentDone: 100,
      resultTableName,
      tick: i,
      elapsedMs: simulatedDurationMs,
      resumedFromLastAttempt: ctx.info.attempt > 1,
    });
  } else {
    while (Date.now() < endAt) {
      ctx.cancellationSignal.throwIfAborted();
      const now = Date.now();
      const elapsedMs = now - startedAt;
      const percentDone = Math.min(99, Math.round((elapsedMs / simulatedDurationMs) * 100));
      const payload: LongQueryHeartbeatPayload = {
        phase: 'running',
        tick: i,
        percentDone,
        resultTableName,
        elapsedMs,
        resumedFromLastAttempt: ctx.info.attempt > 1,
      };
      activity.heartbeat(payload);
      i += 1;
      const remaining = endAt - now;
      if (remaining <= 0) {
        break;
      }
      await sleep(Math.min(heartbeatIntervalMs, remaining));
    }

    const donePayload: LongQueryHeartbeatPayload = {
      phase: 'complete',
      percentDone: 100,
      resultTableName,
      tick: i,
      elapsedMs: Math.min(simulatedDurationMs, Date.now() - startedAt),
    };
    activity.heartbeat({ ...donePayload, resumedFromLastAttempt: ctx.info.attempt > 1 });
  }

  const simulatedRowCount = Math.max(0, Math.floor(simulatedDurationMs / 10));

  return { resultTableName, simulatedRowCount };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
