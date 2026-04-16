// ETL-only Cloudflare utilities — Node.js environment only.
// This file imports Node.js built-ins and must NEVER be imported by
// src/index.ts, src/tools/*, or src/resources/*.
// Only scripts/*.ts should import from this module.

import { execSync } from 'child_process';
import { writeFileSync, unlinkSync } from 'fs';
import { join } from 'path';
import { tmpdir } from 'os';

const d1DatabaseName = process.env.D1_DATABASE_NAME ?? 'bible';

// ─── SQL helpers ──────────────────────────────────────────────────────────────

/**
 * Escapes a SQL value for safe inline embedding in a SQL file.
 * Strings: wrap in single quotes, escape embedded single quotes by doubling.
 * Numbers/booleans: emit as literal.
 * null: emit NULL.
 */
export function sqlLiteral(value: unknown): string {
  if (value === null || value === undefined) return 'NULL';
  if (typeof value === 'boolean') return value ? '1' : '0';
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw new TypeError(`Invalid numeric value: ${value}`);
    }
    return String(value);
  }
  if (typeof value === 'object') {
    throw new TypeError(
      `sqlLiteral does not accept objects; got ${Object.prototype.toString.call(value)}`,
    );
  }
  // string (and fallback for anything else)
  return `'${String(value).replace(/'/g, "''")}'`;
}

const ROWS_PER_INSERT = 200;

/**
 * Builds a multi-row INSERT SQL string from a prefix and an array of row tuples.
 * Groups rows into statements of up to rowsPerInsert rows each (default 200).
 *
 * Example:
 *   buildMultiRowInserts('INSERT INTO t (a,b) VALUES', [[1,'x'],[2,'y']])
 *   → "INSERT INTO t (a,b) VALUES (1, 'x'), (2, 'y');\n"
 */
export function buildMultiRowInserts(
  prefix: string,
  rows: unknown[][],
  rowsPerInsert: number = ROWS_PER_INSERT,
): string {
  const lines: string[] = [];
  for (let start = 0; start < rows.length; start += rowsPerInsert) {
    const chunk = rows.slice(start, start + rowsPerInsert);
    const tuples = chunk.map((row) => `(${row.map(sqlLiteral).join(', ')})`).join(', ');
    lines.push(`${prefix} ${tuples};`);
  }
  return lines.join('\n') + '\n';
}

/**
 * Replaces positional '?' placeholders in a SQL string with inlined literals.
 */
export function inlineParams(sql: string, params: unknown[]): string {
  let paramIndex = 0;
  const result = sql.replace(/\?/g, () => {
    if (paramIndex >= params.length) {
      throw new Error(`SQL has more '?' placeholders than params: ${sql}`);
    }
    return sqlLiteral(params[paramIndex++]);
  });
  if (paramIndex < params.length) {
    console.warn(
      `[cloudflare-etl] inlineParams: ${params.length - paramIndex} extra param(s) unused for SQL: ${sql}`,
    );
  }
  return result;
}

/**
 * Given an array of {sql, params} statements, generates SQL text with
 * parameter values inlined.  Multi-row INSERT statements are grouped up
 * to ROWS_PER_INSERT rows for efficiency.
 *
 * The sql string is expected to be a parameterised INSERT whose VALUES
 * clause contains exactly one row placeholder, e.g.:
 *   INSERT INTO t (a,b) VALUES (?,?)
 *
 * All other statement shapes are emitted as-is with params inlined
 * by positional substitution of '?' placeholders.
 */
export function buildSqlFile(statements: Array<{ sql: string; params?: unknown[] }>): string {
  // Split into INSERT groups vs other statements
  // We detect INSERT statements by looking for VALUES keyword so we can
  // group multiple rows into a single multi-value INSERT.
  const lines: string[] = [];

  let i = 0;
  while (i < statements.length) {
    const stmt = statements[i];
    const upperSql = stmt.sql.trimStart().toUpperCase();

    if (upperSql.startsWith('INSERT')) {
      // Collect a run of INSERT statements with the same leading signature
      // (everything up to VALUES) so we can combine their row tuples.
      const valuesIdx = upperSql.indexOf('VALUES');
      if (valuesIdx === -1) {
        // Malformed INSERT — emit as-is
        lines.push(inlineParams(stmt.sql, stmt.params ?? []) + ';');
        i++;
        continue;
      }

      const prefix = stmt.sql.slice(0, valuesIdx + 'VALUES'.length).trimEnd();
      const run: Array<unknown[]> = [stmt.params ?? []];
      i++;

      // Accumulate consecutive INSERTs with the same prefix
      while (i < statements.length) {
        const next = statements[i];
        const nextUpper = next.sql.trimStart().toUpperCase();
        const nextValuesIdx = nextUpper.indexOf('VALUES');
        if (
          nextValuesIdx !== -1 &&
          next.sql.slice(0, nextValuesIdx + 'VALUES'.length).trimEnd() === prefix
        ) {
          run.push(next.params ?? []);
          i++;
        } else {
          break;
        }
      }

      // Emit in chunks of ROWS_PER_INSERT
      for (let start = 0; start < run.length; start += ROWS_PER_INSERT) {
        const chunk = run.slice(start, start + ROWS_PER_INSERT);
        const tuples = chunk.map((params) => `(${params.map(sqlLiteral).join(', ')})`).join(', ');
        lines.push(`${prefix} ${tuples};`);
      }
    } else {
      lines.push(inlineParams(stmt.sql, stmt.params ?? []) + ';');
      i++;
    }
  }

  return lines.join('\n') + '\n';
}

// ─── D1 ETL client ────────────────────────────────────────────────────────────

/**
 * Executes a raw SQL string via `wrangler d1 execute --file`.
 * Writes the SQL to a temp file, shells out to wrangler, then cleans up.
 * Intended for ETL scripts running locally (not the runtime server).
 */
async function d1BatchFile(sql: string): Promise<void> {
  if (!sql.trim()) return;

  const tmpFile = join(tmpdir(), `d1-batch-${Date.now()}-${process.pid}.sql`);
  try {
    writeFileSync(tmpFile, sql, 'utf8');
    execSync(`npx wrangler d1 execute ${d1DatabaseName} --remote --file="${tmpFile}"`, {
      stdio: 'inherit',
    });
  } finally {
    try {
      unlinkSync(tmpFile);
    } catch {
      // Best-effort cleanup — don't mask the original error
    }
  }
}

/**
 * Bulk-imports an array of parameterised SQL statements via wrangler.
 * Values are inlined into the SQL file; consecutive single-row INSERTs
 * are grouped into multi-value INSERTs (up to 200 rows each).
 * No 100-statement limit — wrangler handles internal batching.
 */
async function d1Batch(statements: Array<{ sql: string; params?: unknown[] }>): Promise<void> {
  if (statements.length === 0) return;

  const sql = buildSqlFile(statements);
  await d1BatchFile(sql);
}

export const d1Etl = {
  batch: d1Batch,
  batchFile: d1BatchFile,
};
