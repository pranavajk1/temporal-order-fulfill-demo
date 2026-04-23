import { proxyActivities } from '@temporalio/workflow';

import type * as activities from '../src/activities';
import type { ExecuteQueryInput, ExecuteQueryResult } from '../src/interfaces/execute-query';
import type { Order } from '../src/interfaces/order';

const { processPayment, reserveInventory, deliverOrder } = proxyActivities<typeof activities>({
    startToCloseTimeout: '65 seconds',
    retry: { nonRetryableErrorTypes: ['CreditCardExpiredException'] }
});

/** PODE-3419 activity #1: long wall-clock work with heartbeats; short heartbeat timeout, long start-to-close. */
const { executeQuery } = proxyActivities<typeof activities>({
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
