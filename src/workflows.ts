import { proxyActivities }
    from '@temporalio/workflow';

import type * as activities from '../src/activities';
import type { Order } from '../src/interfaces/order';

const { processPayment, reserveInventory, deliverOrder } = proxyActivities<typeof activities>({
    startToCloseTimeout: '65 seconds',
    retry: { nonRetryableErrorTypes: ['CreditCardExpiredException'] }
});

export async function OrderFulfillWorkflow(order: Order): Promise<string> {
    const paymentResult = await processPayment(order);
    const inventoryResult = await reserveInventory(order);
    const deliveryResult = await deliverOrder(order);
    return `Order fulfilled: ${paymentResult}, ${inventoryResult}, ${deliveryResult}`;
}
