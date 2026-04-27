import { proxyActivities } from '@temporalio/workflow';

import type * as activities from '../src/activities';
import type {
  DispatchActionsInput,
  DispatchActionsResult,
  PodeQueryPipelineInput,
  PodeQueryPipelineResult,
} from '../src/interfaces/dispatch-actions';
import type { ExecuteQueryInput, ExecuteQueryResult } from '../src/interfaces/execute-query';
import type { Order } from '../src/interfaces/order';

const { processPayment, reserveInventory, deliverOrder } = proxyActivities<typeof activities>({
    startToCloseTimeout: '65 seconds',
    retry: { nonRetryableErrorTypes: ['CreditCardExpiredException'] }
});

/** PODE-3419 activity #1: long wall-clock work with heartbeats; short heartbeat timeout, long start-to-close. */
const { executeQuery, dispatchActions } = proxyActivities<typeof activities>({
    startToCloseTimeout: '30 minutes',
    heartbeatTimeout: '45 seconds',
    retry: { maximumAttempts: 3 },
});

export async function OrderFulfillWorkflow(order: Order): Promise<string> {
    const paymentResult = await processPayment(order);
    const inventoryResult = await reserveInventory(order);
    const deliveryResult = await deliverOrder(order);
    return `Order fulfilled: ${paymentResult}, ${inventoryResult}, ${deliveryResult}`;
}

export async function LongQueryDemoWorkflow(input: ExecuteQueryInput): Promise<ExecuteQueryResult> {
    return executeQuery(input);
}

/** Activity #2 only: checkpointed dispatch + heartbeat resume (in-memory or Postgres by env). */
export async function DispatchResumeDemoWorkflow(
    input: DispatchActionsInput
): Promise<DispatchActionsResult> {
    return dispatchActions(input);
}

/** PODE-3419: executeQuery → dispatchActions, passing logical result table name and row count. */
export async function PodeQueryPipelineWorkflow(
    input: PodeQueryPipelineInput
): Promise<PodeQueryPipelineResult> {
    const { dispatchOptions, ...queryIn } = input;
    const query = await executeQuery(queryIn);
    const d = dispatchOptions ?? {};
    const dispatch = await dispatchActions({
        resultTableName: query.resultTableName,
        runId: queryIn.runId,
        ...d,
        ...(!d.useInventoryTable && { rowCount: d.rowCount ?? query.simulatedRowCount }),
    });
    return { query, dispatch };
}
