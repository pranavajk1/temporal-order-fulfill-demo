import './load-env';
import fs from 'fs/promises';
import { Client, Connection, type WorkflowExecutionInfo } from '@temporalio/client';
import { tsToDate } from '@temporalio/common/lib/time';
import { temporal } from '@temporalio/proto';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import { getEnv } from './interfaces/env';
import type { History } from '@temporalio/common/lib/proto-utils';

const EventType = temporal.api.enums.v1.EventType;

function assertSafeWorkflowType(name: string): void {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
    throw new Error(
      `Invalid WorkflowType for visibility query: ${JSON.stringify(
        name
      )} — use a simple TypeScript export name, e.g. DispatchResumeDemoWorkflow.`
    );
  }
}

/** Not UTC: uses the process default timezone (toLocaleString). */
function localTimeString(d: Date | undefined | null): string {
  if (d == null) {
    return '—';
  }
  return d.toLocaleString(undefined, { dateStyle: 'short', timeStyle: 'medium' });
}

function runDurationSec(start: Date, close: Date | undefined | null): string {
  if (close == null) {
    return '—';
  }
  const ms = close.getTime() - start.getTime();
  if (ms < 0) {
    return '—';
  }
  if (ms < 60_000) {
    return `${(ms / 1000).toFixed(1)} s`;
  }
  const m = Math.floor(ms / 60_000);
  const s = (ms % 60_000) / 1000;
  return `${m}m ${s.toFixed(0)}s`;
}

/** First workflow-task start and first activity start, relative to workflow execution start (pickup / queueing). */
function firstTaskTimesFromHistory(history: History | null | undefined): {
  wft1Ms: number | null;
  act1Ms: number | null;
} {
  const events = history?.events;
  if (!events?.length) {
    return { wft1Ms: null, act1Ms: null };
  }
  let tExec: number | null = null;
  let tWft: number | null = null;
  let tAct: number | null = null;
  for (const e of events) {
    if (e.eventType == null || e.eventTime == null) {
      continue;
    }
    const t = tsToDate(e.eventTime).getTime();
    if (e.eventType === EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED) {
      tExec = t;
    } else if (e.eventType === EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED && tWft == null) {
      tWft = t;
    } else if (e.eventType === EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED && tAct == null) {
      tAct = t;
    }
  }
  if (tExec == null) {
    return { wft1Ms: null, act1Ms: null };
  }
  return {
    wft1Ms: tWft != null ? tWft - tExec : null,
    act1Ms: tAct != null ? tAct - tExec : null,
  };
}

async function connectFromEnv(): Promise<Client> {
  const { address, namespace, clientCertPath, clientKeyPath, clientApiKey, serverNameOverride, serverRootCACertificatePath } =
    getEnv();

  let connection;
  if (clientCertPath && clientKeyPath) {
    const serverRootCACertificate = serverRootCACertificatePath
      ? await fs.readFile(serverRootCACertificatePath)
      : undefined;
    connection = await Connection.connect({
      address,
      tls: {
        serverNameOverride,
        serverRootCACertificate,
        clientCertPair: {
          crt: await fs.readFile(clientCertPath),
          key: await fs.readFile(clientKeyPath),
        },
      },
    });
  } else if (clientApiKey) {
    connection = await Connection.connect({
      address,
      tls: true,
      apiKey: clientApiKey,
      metadata: { 'temporal-namespace': namespace },
    });
  } else {
    connection = await Connection.connect({ address });
  }
  return new Client({ connection, namespace });
}

async function listLastCompleted(
  client: Client,
  workflowType: string,
  limit: number
): Promise<WorkflowExecutionInfo[]> {
  const query = `ExecutionStatus = "Completed" AND WorkflowType = "${workflowType}" ORDER BY CloseTime DESC`;
  const out: WorkflowExecutionInfo[] = [];
  try {
    for await (const w of client.workflow.list({ query, pageSize: limit })) {
      out.push(w);
      if (out.length >= limit) {
        break;
      }
    }
    return out;
  } catch (e) {
    console.warn(
      'List query with ORDER BY is not supported on this server; using a looser filter and sorting in process.\n',
      (e as Error).message ?? e
    );
    const queryLoose = `ExecutionStatus = "Completed" AND WorkflowType = "${workflowType}"`;
    const buf: WorkflowExecutionInfo[] = [];
    for await (const w of client.workflow.list({ query: queryLoose, pageSize: 2000 })) {
      if (w.type === workflowType) {
        buf.push(w);
      }
    }
    buf.sort((a, b) => (b.closeTime?.getTime() ?? 0) - (a.closeTime?.getTime() ?? 0));
    return buf.slice(0, limit);
  }
}

async function main(): Promise<void> {
  const argv = yargs(hideBin(process.argv))
    .options({
      count: { type: 'number', default: 25, alias: 'c', description: 'How many completed runs to show (most recent first)' },
      workflowType: {
        type: 'string',
        default: 'DispatchResumeDemoWorkflow',
        description: 'Workflow type name as registered in the worker',
      },
      pickup: {
        type: 'boolean',
        default: true,
        description: 'Fetch history to measure delay until first workflow task / first activity start (extra RPCs)',
      },
    })
    .strict()
    .parse() as { count: number; workflowType: string; pickup: boolean };

  assertSafeWorkflowType(argv.workflowType);
  const limit = Math.max(1, argv.count);
  const tz = Intl.DateTimeFormat().resolvedOptions().timeZone;

  const client = await connectFromEnv();
  try {
    const rows = await listLastCompleted(client, argv.workflowType, limit);
    if (rows.length === 0) {
      console.log(
        `No completed workflows of type ${argv.workflowType} found. Check WorkflowType, namespace, or run some workflows first.`
      );
      return;
    }

    const pickups: { wft1Ms: number | null; act1Ms: number | null }[] = [];
    if (argv.pickup) {
      for (const w of rows) {
        const handle = client.workflow.getHandle(w.workflowId, w.runId);
        const history = await handle.fetchHistory();
        pickups.push(firstTaskTimesFromHistory(history));
      }
    }

    console.log(
      `Last ${rows.length} completed: ${argv.workflowType}\n` +
        `Display times are local to this machine (${tz}), not UTC.\n`
    );

    const wIdW = 42;
    const pad = (s: string, w: number) => (s.length > w ? s.slice(0, w - 2) + '…' : s.padEnd(w));
    if (argv.pickup) {
      const hdr =
        '#'.padEnd(3) +
        pad('workflowId', wIdW) +
        ' start (local)          end (local)            run      wft1  act1';
      const u = '---'.padEnd(3) + '-'.repeat(Math.max(hdr.length - 3, 40));
      console.log([hdr, u].join('\n'));
    } else {
      const hdr = '#'.padEnd(3) + pad('workflowId', wIdW) + ' start (local)          end (local)            run';
      const u = '---'.padEnd(3) + '-'.repeat(Math.max(hdr.length - 3, 40));
      console.log([hdr, u].join('\n'));
    }

    for (let i = 0; i < rows.length; i++) {
      const w = rows[i];
      if (w == null) {
        continue;
      }
      const p = argv.pickup ? pickups[i] : undefined;
      const wft1 = argv.pickup && p ? (p.wft1Ms == null ? '  —' : String(p.wft1Ms).padStart(5)) : '';
      const act1 = argv.pickup && p ? (p.act1Ms == null ? '  —' : String(p.act1Ms).padStart(5)) : '';
      const run = runDurationSec(w.startTime, w.closeTime);
      if (argv.pickup) {
        console.log(
          String(i + 1).padEnd(3) +
            pad(w.workflowId, wIdW) +
            ' ' +
            localTimeString(w.startTime) +
            '  ' +
            localTimeString(w.closeTime) +
            '  ' +
            run.padStart(5) +
            (wft1 ? '  ' + wft1 : '') +
            (act1 ? '  ' + act1 : '')
        );
      } else {
        console.log(
          String(i + 1).padEnd(3) +
            pad(w.workflowId, wIdW) +
            ' ' +
            localTimeString(w.startTime) +
            '  ' +
            localTimeString(w.closeTime) +
            '  ' +
            run.padStart(5)
        );
      }
    }

    if (argv.pickup) {
      const wfts = pickups.map((p) => p.wft1Ms).filter((x): x is number => x != null);
      const acts = pickups.map((p) => p.act1Ms).filter((x): x is number => x != null);
      if (wfts.length || acts.length) {
        console.log('');
        console.log(
          'wft1 = ms from execution start to first workflow task start (worker took the workflow task).  ' +
            'act1 = ms to first activity task start (dispatch work actually running on the activity worker).'
        );
        if (wfts.length) {
          const a = wfts.reduce((a, b) => a + b, 0) / wfts.length;
          console.log(
            `wft1: min ${Math.min(...wfts)} ms, max ${Math.max(...wfts)} ms, avg ${a.toFixed(0)} ms  (${(a / 1000).toFixed(2)} s average)`
          );
        }
        if (acts.length) {
          const a = acts.reduce((a, b) => a + b, 0) / acts.length;
          console.log(
            `act1: min ${Math.min(...acts)} ms, max ${Math.max(...acts)} ms, avg ${a.toFixed(0)} ms  (${(a / 1000).toFixed(2)} s average)`
          );
        }
      }
    } else {
      console.log('\nRe-run with default --pickup to add wft1 / act1 (pickup) columns from event history.');
    }
  } finally {
    await client.connection.close();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
