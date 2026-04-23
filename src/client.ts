import fs from 'fs/promises';
import path from 'path';
import { Connection, Client } from '@temporalio/client';
import { runWorkflows, getDefaultOrders } from './starter';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import type { Order, OrderItem } from './interfaces/order';
import { NativeConnection } from '@temporalio/worker';
import { Env, getEnv } from './interfaces/env';

// Extended Env interface to include API key
interface EnvWithApiKey extends Env {
  clientApiKey?: string;
}

/**
 * Schedule a Workflow connecting with either mTLS or API key authentication.
 * Configuration is provided via environment variables.
 * 
 * For mTLS: Requires clientCertPath and clientKeyPath
 * For API key: Requires clientApiKey
 * Note that serverNameOverride and serverRootCACertificate are optional.
 */
async function run({
  address,
  namespace,
  clientCertPath,
  clientKeyPath,
  clientApiKey,
  serverNameOverride,
  serverRootCACertificatePath,
  taskQueue,
}: EnvWithApiKey, numOrders?: number, invalidPercentage?: number) {
  let client: Client;
  let connection: Connection | NativeConnection;

  // Check for mTLS certificates first
  if (clientCertPath && clientKeyPath) {
    console.log('Using mTLS authentication');
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
  }
  // If no mTLS certificates, check for API key
  else if (clientApiKey) {
    console.log('Using API key authentication');
    connection = await Connection.connect({
      address,
      tls: true,
      apiKey: clientApiKey,
      metadata: {
        'temporal-namespace': namespace,
      },
    });
  }
  // Fallback to unencrypted connection (not recommended for production)
  else {
    console.log('Warning: Using unencrypted connection');
    connection = await Connection.connect({ address });
  }

  // Generate orders
  const orders = numOrders && invalidPercentage !== undefined 
    ? generateOrders(numOrders, invalidPercentage) 
    : getDefaultOrders();

  client = new Client({ connection, namespace });
  await runWorkflows(client, taskQueue, orders);
  // const handle = await client.workflow.getHandle('order-fulfill-0-1776954337310');
  // await handle.terminate();
  // console.log('Schedule deleted');
  // 1. List every schedule in this namespace
  // console.log('=== Workflows ===\n');
  // const workflows = [];
  // for await (const s of client.workflow.list()) {
  //   workflows.push(s);
  // }

  // if (workflows.length === 0) {
  //   console.log('No schedules found.');
  //   return;
  // }

  // for (const workflow of workflows) {
  //   console.log(`\n--- Workflow: ${workflow.workflowId} ---`);
  //   const handle = client.workflow.getHandle(workflow.workflowId);
  //   const query = `TemporalScheduledById = "${workflow.workflowId}"`;
  //   const results = await handle.query(query);
  //   console.log(results);
  // }
  
}

// Rest of the code remains unchanged
function getRandomInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function generateOrders(count: number, invalidPercentage: number): Order[] {
  const stockDatabasePath = path.resolve(__dirname, '../data/stock_database.json');
  const stockData = require(stockDatabasePath);
  const orders: Order[] = [];
  const numInvalidOrders = Math.floor((invalidPercentage / 100) * count);

  for (let i = 0; i < count; i++) {
    const numItems = getRandomInt(1, 3);
    const items: OrderItem[] = [];

    for (let j = 0; j < numItems; j++) {
      const itemIndex = getRandomInt(0, stockData.length - 1);
      const item = stockData[itemIndex];
      items.push({
        itemName: item.itemName,
        itemPrice: item.itemPrice,
        quantity: getRandomInt(1, 3),
      });
    }

    const order: Order = {
      items: items,
      payment: {
        creditCard: {
          number: "1234 5678 1234 5678",
          expiration: "12/25"
        }
      }
    };

    if (i < numInvalidOrders) {
      makeOrderInvalid(order);
    }

    orders.push(order);
  }

  return orders;
}

function makeOrderInvalid(order: Order): void {
  if (order.items.length > 0) {
    order.items[0].itemName += "@@@";
  }
}

const argv = yargs(hideBin(process.argv)).options({
  numOrders: { type: 'number', alias: 'n' },
  invalidPercentage: { type: 'number', alias: 'i' }
}).argv as { numOrders?: number, invalidPercentage?: number };

run(getEnv(), argv.numOrders, argv.invalidPercentage).then(
  () => process.exit(0),
  (err) => {
    console.error(err);
    process.exit(1);
  }
);