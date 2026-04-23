import { Client } from '@temporalio/client';
import { LongQueryDemoWorkflow, OrderFulfillWorkflow } from './workflows';
import type { ExecuteQueryInput } from './interfaces/execute-query';
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

const defaultLongQueryInput: ExecuteQueryInput = {
  partnerId: 'demo_partner',
  queryId: 'q1',
  runId: 'run_placeholder',
  simulatedDurationMs: 90_000,
  heartbeatIntervalMs: 4_000,
};

/** Run only `LongQueryDemoWorkflow` to exercise long activity #1 + heartbeats in the UI. */
export async function startLongQueryDemo(
  client: Client,
  taskQueue: string,
  input: Partial<ExecuteQueryInput> = {}
): Promise<void> {
  const runId = input.runId ?? `run_${Date.now()}`;
  const args: [ExecuteQueryInput] = [
    { ...defaultLongQueryInput, ...input, runId },
  ];
  const handle = await client.workflow.start(LongQueryDemoWorkflow, {
    taskQueue,
    workflowId: `long-query-demo-${Date.now()}`,
    args,
  });
  console.log('Started LongQueryDemoWorkflow', handle.workflowId);
  const result = await handle.result();
  console.log('LongQueryDemoWorkflow completed:', result);
}
