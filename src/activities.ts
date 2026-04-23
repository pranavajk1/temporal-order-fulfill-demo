import { Order } from './interfaces/order';
import type { ExecuteQueryInput, ExecuteQueryResult } from './interfaces/execute-query';
import type { DispatchActionsInput, DispatchActionsResult } from './interfaces/dispatch-actions';
import { reserveInventory as reserveInventoryAPI } from './api';
import { simulateLongQuery } from './simulate-long-query';
import { dispatchActions as runDispatch } from './dispatch-actions';

/** Activity #1 — long “query”; implemented as timed sleep + heartbeats (see simulate-long-query.ts). */
export async function executeQuery(input: ExecuteQueryInput): Promise<ExecuteQueryResult> {
  return simulateLongQuery(input);
}

/** Activity #2 — read result rows, dispatch with heartbeat checkpoints; resume on retry (see dispatch-actions.ts). */
export async function dispatchActions(input: DispatchActionsInput): Promise<DispatchActionsResult> {
  return runDispatch(input);
}

export async function requireApproval(order: Order): Promise<boolean> {
  console.log(`Checking order requires approval (over $10k)`);

  // Simulate approval logic
  if (order.items.reduce((sum, item) =>
    sum + item.itemPrice * item.quantity, 0) > 10000) {
    console.log('Order requires approval');
    return true;
  }

  await simulateDelay(1000);
  return false;
}

export async function processPayment(order: Order): Promise<string> {
  console.log("Processing payment...");

  // Simulate payment processing logic
  if (order.payment.creditCard.expiration === "12/23") {
    throw new CreditCardExpiredException("Payment failed: Credit card expired");
  }

  await simulateDelay(1000);
  return `Payment processed for ${order.items.length} items`;
}

export async function reserveInventory(order: Order): Promise<string> {
  
  // // Simulate inventory service downtime
  // // The activity will sleep the first 3 times it is called
  // // And throw an error to simulate API call timeout
  // const { attempt } = activity.Context.current().info;
  // if (attempt <= 4) {
  //   console.log(`Inventory service down, attempt ${attempt}`);
  //   await new Promise((resolve) => setTimeout(resolve, 10000));
  //   throw new Error("Inventory service down");
  // }

  // Simulate inventory reservation logic
  console.log("Reserving inventory...");
  await reserveInventoryAPI(order.items);

  await simulateDelay(20000);
  return `Inventory reserved for ${order.items.length} items`;
}

export async function deliverOrder(order: Order): Promise<string> {
  // Simulate order delivery logic
  console.log("Delivering order...");

  await simulateDelay(30000);
  return `Order delivered for ${order.items.length} items`;
}

function simulateDelay(sleepMs: number): Promise<void> {
  // take sleepMs as input and introduce variance of +/- 20%
  const variance = sleepMs * 0.2;
  sleepMs += Math.floor(Math.random() * 2 * variance) - variance;
  console.log(`Simulating delay of ${sleepMs}ms`);
  return new Promise((resolve) => setTimeout(resolve, sleepMs));
}

export class CreditCardExpiredException extends Error {
  constructor(message?: string) {
    super(message);
    Object.setPrototypeOf(this, new.target.prototype);
    this.name = CreditCardExpiredException.name;
  }
}