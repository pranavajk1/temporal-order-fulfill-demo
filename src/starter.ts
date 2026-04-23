import { Client } from '@temporalio/client';
import { OrderFulfillWorkflow } from './workflows';
import type { Order } from './interfaces/order';

const sampleOrders: Order[] = [
  {
    items: [
      { itemName: "Cloudmonster Running Shoe (Men)", itemPrice: 126.99, quantity: 1 },
      { itemName: "2002R Sneaker (Men)", itemPrice: 63.00, quantity: 2 }
    ],
    payment: {
      creditCard: {
        number: "5678 1234 5678 1234",
        expiration: "12/26"
      }
    }
  }
];

export async function runWorkflows(client: Client, taskQueue: string, orders: Order[]): Promise<void> {
  const workflowPromises = orders.map((order, index) =>
    // client.workflow.execute(OrderFulfillWorkflow, {
    //   taskQueue,
    //   workflowId: `order-fulfill-${index}-${Date.now()}`,
    //   args: [order],
    // }).then(
    //   result => ({ status: 'fulfilled', result }),
    //   error => ({ status: 'rejected', reason: error })
    // )

    client.workflow.start(OrderFulfillWorkflow, {
      taskQueue,
      workflowId: `order-fulfill-${index}-${Date.now()}`,
      args: [order],
    }).then(
      result => ({ status: 'fulfilled', result }),
      error => ({ status: 'rejected', reason: error })
    )

    // client.schedule.create({
    //   scheduleId: `order-fulfill-every-40d-${index}-${Date.now()}`,
    //   spec: {
    //     intervals: [{ every: '40d' }], // or: every: 40 * 24 * 60 * 60 * 1000
    //   },
    //   action: {
    //     type: 'startWorkflow',
    //     workflowType: 'OrderFulfillWorkflow', // or your registered name
    //     taskQueue: taskQueue,
    //     args: [order],
    //   },
    // }).then(
    //   result => ({ status: 'fulfilled', result }),
    //   error => ({ status: 'rejected', reason: error })
    // )
  );

  const results = await Promise.allSettled(workflowPromises);

  results.forEach((result, index) => {
    if (result.status === 'fulfilled') {
      console.log(`Workflow ${index + 1} succeeded with result:`, result.value);
    } else {
      console.error(`Workflow ${index + 1} failed with reason:`, result.reason);
    }
  });
}

export function getDefaultOrders(): Order[] {
  return sampleOrders;
}
