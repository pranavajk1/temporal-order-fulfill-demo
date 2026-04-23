import { Client } from '@temporalio/client';
import {
  DispatchResumeDemoWorkflow,
  LongQueryDemoWorkflow,
  OrderFulfillWorkflow,
  PodeQueryPipelineWorkflow,
} from './workflows';
import type { ExecuteQueryInput } from './interfaces/execute-query';
import type { DispatchActionsInput, PodeQueryPipelineInput } from './interfaces/dispatch-actions';
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

const defaultDispatchInput: DispatchActionsInput = {
  resultTableName: 'demo_partner_q1_dispatch_result',
  runId: 'run_placeholder',
  rowCount: 32,
  rowsPerHeartbeat: 4,
  perRowSimulatedMs: 1,
  /** Fails on the 3rd row (ord 2); first retry resumes at nextRowOrd 3 (see heartbeats in UI). */
  failOnRowIndex: 2,
};

/**
 * Run only `DispatchResumeDemoWorkflow` — simulates “millions of rows” dispatch via
 * `heartbeatDetails` + resume; uses Postgres when `DATABASE_URL` is set, else in-memory.
 */
export async function startDispatchResumeDemo(
  client: Client,
  taskQueue: string,
  input: Partial<DispatchActionsInput> = {}
): Promise<void> {
  const runId = input.runId ?? `run_${Date.now()}`;
  const resultTableName = input.resultTableName ?? `demo_partner_q1_${runId}_result`;
  const args: [DispatchActionsInput] = [
    { ...defaultDispatchInput, ...input, runId, resultTableName },
  ];
  const handle = await client.workflow.start(DispatchResumeDemoWorkflow, {
    taskQueue,
    workflowId: `dispatch-resume-demo-${Date.now()}`,
    args,
  });
  console.log('Started DispatchResumeDemoWorkflow', handle.workflowId);
  const result = await handle.result();
  console.log('DispatchResumeDemoWorkflow completed:', result);
}

/** executeQuery (short) + dispatch; optional `failOnRowIndex` on dispatch via `dispatchOptions`. */
export async function startPodeQueryPipelineDemo(
  client: Client,
  taskQueue: string,
  input: Partial<PodeQueryPipelineInput> = {}
): Promise<void> {
  const runId = input.runId ?? `run_${Date.now()}`;
  const { dispatchOptions: inDispatch, runId: _run, ...inputRest } = input;
  const args: [PodeQueryPipelineInput] = [
    {
      partnerId: 'demo_partner',
      queryId: 'q1',
      simulatedDurationMs: 5_000,
      heartbeatIntervalMs: 1_000,
      ...inputRest,
      runId,
      dispatchOptions: {
        rowsPerHeartbeat: 10,
        perRowSimulatedMs: 1,
        ...inDispatch,
      },
    },
  ];
  const handle = await client.workflow.start(PodeQueryPipelineWorkflow, {
    taskQueue,
    workflowId: `pode-3419-pipeline-${Date.now()}`,
    args,
  });
  console.log('Started PodeQueryPipelineWorkflow', handle.workflowId);
  const result = await handle.result();
  console.log('PodeQueryPipelineWorkflow completed:', result);
}
