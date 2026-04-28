import './load-env';
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

/** `PODE_DISPATCH_USE_SEED=1` (or `true` / `yes`) -> tiny `pode_result_rows` + failure demo, not your real `inventory_items`. */
function useSeedTableEnv(): boolean {
  const v = process.env.PODE_DISPATCH_USE_SEED;
  return v === '1' || v === 'true' || v === 'yes';
}

/**
 * By default, dispatch scans `public.inventory_items` (keyset on `id`) as long as the **worker** has
 * `DATABASE_URL` in `.env` (see `worker.ts` / `import 'dotenv/config'`). Opt out: `PODE_DISPATCH_USE_SEED=1`.
 */
function defaultDispatchInputBase(): DispatchActionsInput {
  if (useSeedTableEnv()) {
    return {
      resultTableName: 'demo_partner_q1_dispatch_result',
      runId: 'run_placeholder',
      useInventoryTable: false,
      rowCount: 32,
      rowsPerHeartbeat: 4,
      perRowSimulatedMs: 1,
      failOnRowIndex: 2,
    };
  }
  return {
    resultTableName: 'demo_dispatch_logical',
    runId: 'run_placeholder',
    useInventoryTable: true,
    rowsPerHeartbeat: 2_000,
    perRowSimulatedMs: 0,
    batchSize: 2_000,
  };
}

/**
 * Run only `DispatchResumeDemoWorkflow` — by default `useInventoryTable: true` and keyset scan of
 * `public.inventory_items` (requires `DATABASE_URL` in the **worker** `.env`). Set `PODE_DISPATCH_USE_SEED=1` for a tiny in-memory/seed run.
 */
export async function startDispatchResumeDemo(
  client: Client,
  taskQueue: string,
  input: Partial<DispatchActionsInput> = {}
): Promise<void> {
  const runId = input.runId ?? `run_${Date.now()}`;
  const resultTableName = input.resultTableName ?? `demo_partner_q1_${runId}_result`;
  const args: [DispatchActionsInput] = [
    { ...defaultDispatchInputBase(), ...input, runId, resultTableName },
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

/**
 * Start many `DispatchResumeDemoWorkflow` runs (e.g. scale / queueing tests). Submits to Temporal
 * only; does not await results — compare worker `activity.log` / UI for when each is picked.
 */
export async function startDispatchResumeDemos(
  client: Client,
  taskQueue: string,
  count: number,
  input: Partial<DispatchActionsInput> = {}
): Promise<void> {
  if (count < 1) {
    throw new Error('startDispatchResumeDemos: count must be at least 1');
  }
  const batchT0 = Date.now();
  const base = `scale_${batchT0}`;
  const { runId: inputRunId, resultTableName: inputResultTable, ...inputRest } = input;
  const startPromises = Array.from({ length: count }, (_, i) => {
    const runId = inputRunId != null ? `${String(inputRunId)}_${i}` : `run_${base}_w${i}`;
    const resultTableName = inputResultTable ?? `demo_partner_q1_${runId}_result`;
    const args: [DispatchActionsInput] = [
      { ...defaultDispatchInputBase(), ...inputRest, runId, resultTableName },
    ];
    const workflowId = `dispatch-resume-scale-w${i}-${base}`;
    return client.workflow
      .start(DispatchResumeDemoWorkflow, {
        taskQueue,
        workflowId,
        args,
      })
      .then((h) => ({ i, workflowId, handle: h, tAckMs: Date.now() - batchT0 }));
  });

  const tSubmit0 = Date.now();
  const acks = await Promise.all(startPromises);
  const totalSubmitMs = Date.now() - tSubmit0;

  const firstAckMs = Math.min(...acks.map((a) => a.tAckMs));
  const lastAckMs = Math.max(...acks.map((a) => a.tAckMs));
  console.log(
    `DispatchResume scale: submitted ${count} workflows, server accept spread ${firstAckMs}..${lastAckMs}ms from batch t0, Promise.all ${totalSubmitMs}ms`
  );
  acks.sort((a, b) => a.i - b.i);
  for (const a of acks) {
    console.log(`  [${a.i}] ${a.workflowId}  (accept +${a.tAckMs}ms)`);
  }
  console.log(
    'Not awaiting results. With one worker, watch pickup delay via worker activity logs (dispatchActions) or the Temporal UI.'
  );
}

/** executeQuery (short) + dispatch; optional `failOnRowIndex` on dispatch via `dispatchOptions`. */
export async function startPodeQueryPipelineDemo(
  client: Client,
  taskQueue: string,
  input: Partial<PodeQueryPipelineInput> = {}
): Promise<void> {
  const runId = input.runId ?? `run_${Date.now()}`;
  const { dispatchOptions: inDispatch, runId: _run, ...inputRest } = input;
  const seed = useSeedTableEnv();
  const args: [PodeQueryPipelineInput] = [
    {
      partnerId: 'demo_partner',
      queryId: 'q1',
      simulatedDurationMs: 5_000,
      heartbeatIntervalMs: 1_000,
      ...inputRest,
      runId,
      dispatchOptions: {
        useInventoryTable: !seed,
        rowsPerHeartbeat: seed ? 10 : 2_000,
        perRowSimulatedMs: seed ? 1 : 0,
        batchSize: 2_000,
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
