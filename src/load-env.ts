import path from 'path';
import { config } from 'dotenv';

/**
 * Load `.env` from the process cwd (repo root when you `npm run start` / `workflow`).
 * `override: true` so a leftover `DATABASE_URL` in the shell/IDE does not win over the file
 * (a common reason “I tested with .env but the worker used another database”).
 */
config({ path: path.resolve(process.cwd(), '.env'), override: true });

/** Log which DB host/db name the process will use (no user/password). */
export function logDatabaseUrlTargetIfSet(): void {
  const u = process.env.DATABASE_URL;
  if (!u) {
    return;
  }
  try {
    const url = new URL(u);
    const db = url.pathname.replace(/^\//, '') || '(default)';
    // eslint-disable-next-line no-console
    console.log(
      `[load-env] DATABASE_URL -> host=${url.hostname} port=${url.port || 5432} database=${db}`
    );
  } catch {
    // eslint-disable-next-line no-console
    console.log('[load-env] DATABASE_URL is set (could not parse for logging)');
  }
}

logDatabaseUrlTargetIfSet();
