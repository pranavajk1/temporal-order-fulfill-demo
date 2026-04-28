/**
 * Export a Postgres table to CSV using DATABASE_URL and optional PODE_DISPATCH_* .env
 * (same as dispatch: schema, table, max rows). By default: public.inventory_items.
 *
 * Uses a server-side cursor + batched FETCH so the full result is not held in JS memory
 * (avoids V8 "heap out of memory" on large tables).
 *
 * Usage: npm run export-csv
 * Optional: PODE_CSV_PATH, PODE_CSV_FETCH (rows per FETCH, default 10000)
 * Optional: NODE_OPTIONS=--max-old-space-size=... (see npm script)
 * Logs wall time for the whole export (pool + cursor + file write) on completion.
 */
import fs from 'fs';
import { performance } from 'node:perf_hooks';
import path from 'path';
import type { FieldDef, Pool, PoolClient, QueryResult } from 'pg';
import { Pool as PgPool } from 'pg';
import './load-env';

const CURSOR_NAME = 'export_pode_to_csv_c';

function assertSafeSqlIdent(name: string, what: string): void {
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
    throw new Error(`export-to-csv: invalid ${what} ${JSON.stringify(name)} (use simple identifiers only)`);
  }
}

function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

function maxRowsFromEnv(): number {
  const r = process.env.PODE_DISPATCH_MAX_ROWS;
  if (r == null || r === '') {
    return 0;
  }
  const n = parseInt(r, 10);
  return Number.isFinite(n) && n >= 0 ? n : 0;
}

function fetchSizeFromEnv(): number {
  const r = process.env.PODE_CSV_FETCH;
  if (r == null || r === '') {
    return 10_000;
  }
  const n = parseInt(r, 10);
  return Number.isFinite(n) && n > 0 ? n : 10_000;
}

function cellToString(value: unknown): string {
  if (value == null) {
    return '';
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (typeof value === 'string') {
    return value;
  }
  if (typeof value === 'number' || typeof value === 'boolean' || typeof value === 'bigint') {
    return String(value);
  }
  if (Buffer.isBuffer(value)) {
    return value.toString('base64');
  }
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function escapeCsvField(s: string): string {
  if (/[",\n\r]/.test(s) || s.includes(',')) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function writeCsvDataLines(
  fields: FieldDef[] | { name: string }[],
  rows: Record<string, unknown>[]
): string {
  return rows
    .map(
      (row) => `${fields.map((f) => escapeCsvField(cellToString(row[f.name]))).join(',')}\n`
    )
    .join('');
}

function writeAll(w: fs.WriteStream, chunk: string): Promise<void> {
  return new Promise((resolve, reject) => {
    w.write(chunk, 'utf8', (err) => (err != null ? reject(err) : resolve()));
  });
}

function defaultOutPath(schema: string, table: string): string {
  const ts = new Date();
  const pad = (n: number) => n.toString().padStart(2, '0');
  const stamp = `${ts.getFullYear()}${pad(ts.getMonth() + 1)}${pad(ts.getDate())}-${pad(
    ts.getHours()
  )}${pad(ts.getMinutes())}${pad(ts.getSeconds())}`;
  return path.join(process.cwd(), `export-${schema}-${table}-${stamp}.csv`);
}

function buildSelectForCursor(
  schema: string,
  table: string,
  maxRows: number
): { sql: string; description: string } {
  const t = `${quoteIdent(schema)}.${quoteIdent(table)}`;
  if (maxRows > 0) {
    return {
      sql: `SELECT * FROM (SELECT * FROM ${t} LIMIT ${maxRows}) AS _export_limited_sub`,
      description: `SELECT * FROM ${t} LIMIT ${maxRows}`,
    };
  }
  return {
    sql: `SELECT * FROM ${t}`,
    description: `SELECT * FROM ${t}`,
  };
}

async function streamTableToCsv(
  pool: Pool,
  outPath: string,
  schema: string,
  table: string,
  maxRows: number
): Promise<number> {
  const fetchN = fetchSizeFromEnv();
  const { sql: selectSql, description } = buildSelectForCursor(schema, table, maxRows);

  // eslint-disable-next-line no-console
  console.log(`[export-to-csv] query: ${description.replace(/\s+/g, ' ').trim()} (FETCH ${fetchN})`);

  const w = fs.createWriteStream(outPath, { flags: 'w' });
  const client: PoolClient = await pool.connect();
  let rowCount = 0;

  try {
    // Column layout without pulling rows; safe for any table size.
    const head: QueryResult<Record<string, unknown>> = await client.query(
      `SELECT * FROM ${quoteIdent(schema)}.${quoteIdent(table)} LIMIT 0`
    );
    const { fields } = head;
    if (!fields.length) {
      throw new Error('export-to-csv: no columns in table');
    }

    const headerLine = `${fields.map((f) => escapeCsvField(f.name)).join(',')}\n`;
    await writeAll(w, headerLine);

    try {
      await client.query('BEGIN');
      // Cursor name is a constant safe identifier, not user input.
      await client.query(`DECLARE ${CURSOR_NAME} NO SCROLL CURSOR FOR ${selectSql}`);

      for (;;) {
        const r: QueryResult<Record<string, unknown>> = await client.query(
          `FETCH ${fetchN} FROM ${CURSOR_NAME}`
        );
        if (r.rows.length === 0) {
          break;
        }
        rowCount += r.rows.length;
        const block = writeCsvDataLines(r.fields.length ? r.fields : fields, r.rows);
        await writeAll(w, block);
        if (r.rows.length < fetchN) {
          break;
        }
      }

      await client.query(`CLOSE ${CURSOR_NAME}`);
      await client.query('COMMIT');
    } catch (e) {
      try {
        await client.query('ROLLBACK');
      } catch {
        // best effort
      }
      throw e;
    }
  } finally {
    client.release();
    await new Promise<void>((resolve, reject) => {
      w.once('error', reject);
      w.end((err: Error | undefined) => (err != null ? reject(err) : resolve()));
    });
  }

  return rowCount;
}

async function main(): Promise<void> {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('export-to-csv: DATABASE_URL is not set in .env');
  }

  const schema = process.env.PODE_DISPATCH_SCHEMA ?? 'public';
  const table = process.env.PODE_DISPATCH_TABLE ?? 'inventory_items';
  assertSafeSqlIdent(schema, 'schema');
  assertSafeSqlIdent(table, 'table');

  const maxRows = maxRowsFromEnv();
  const outPath = process.env.PODE_CSV_PATH
    ? path.resolve(process.env.PODE_CSV_PATH)
    : defaultOutPath(schema, table);

  // eslint-disable-next-line no-console
  console.log(`[export-to-csv] writing -> ${outPath}`);

  const t0 = performance.now();
  const pool = new PgPool({ connectionString, max: 2 });
  try {
    const rowCount = await streamTableToCsv(pool, outPath, schema, table, maxRows);
    const elapsedMs = Math.round(performance.now() - t0);
    const elapsedSec = (elapsedMs / 1000).toFixed(2);
    // eslint-disable-next-line no-console
    console.log(
      `[export-to-csv] done (${rowCount} rows) in ${elapsedSec}s (${elapsedMs}ms)`
    );
  } finally {
    await pool.end();
  }
}

main().catch((e) => {
  // eslint-disable-next-line no-console
  console.error(e);
  process.exit(1);
});
