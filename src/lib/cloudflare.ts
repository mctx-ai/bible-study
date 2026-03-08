// Cloudflare HTTP REST API client
// Reads config from environment at module scope (nodejs_compat mode)

import { execSync } from 'child_process';
import { writeFileSync, unlinkSync } from 'fs';
import { join } from 'path';
import { tmpdir } from 'os';

const apiToken =
  process.env.BIBLE_API_TOKEN ?? process.env.CLOUDFLARE_API_TOKEN ?? '';
const accountId =
  process.env.BIBLE_ACCOUNT_ID ?? process.env.CLOUDFLARE_ACCOUNT_ID ?? '';
const databaseId = process.env.D1_DATABASE_ID ?? '';
const indexName = process.env.VECTORIZE_INDEX_NAME ?? '';
const d1DatabaseName = process.env.D1_DATABASE_NAME ?? 'bible';

const BASE = 'https://api.cloudflare.com/client/v4';

// ─── Shared types ─────────────────────────────────────────────────────────────

export interface D1RowMeta {
  changes: number;
  rows_read: number;
  rows_written?: number;
  last_row_id?: number;
  changed_db?: boolean;
  duration?: number;
  size_after?: number;
}

export interface D1ResultSet {
  results: Record<string, unknown>[];
  meta: D1RowMeta;
  success: boolean;
}

export type D1Result = D1ResultSet;

export interface VectorizeMatch {
  id: string;
  score: number;
  values?: number[];
  metadata?: Record<string, unknown>;
}

// ─── Shared helpers ───────────────────────────────────────────────────────────

function authHeaders(): Record<string, string> {
  return {
    Authorization: `Bearer ${apiToken}`,
    'Content-Type': 'application/json',
  };
}

async function cfFetch<T>(url: string, init: RequestInit): Promise<T> {
  const res = await fetch(url, {
    ...init,
    headers: {
      ...authHeaders(),
      ...(init.headers as Record<string, string> | undefined),
    },
  });

  if (!res.ok) {
    const body = await res.text().catch(() => '');
    console.error('Cloudflare API error:', res.status, body);
    throw new Error(`Cloudflare API error: ${res.status} ${res.statusText}`);
  }

  const json = (await res.json()) as {
    success: boolean;
    errors: Array<{ code: number; message: string }>;
    result: T;
  };

  if (!json.success) {
    const msg = json.errors.map((e) => `[${e.code}] ${e.message}`).join('; ');
    throw new Error(`Cloudflare API returned success=false: ${msg}`);
  }

  return json.result;
}

// ─── D1 client ────────────────────────────────────────────────────────────────

const d1Base = `${BASE}/accounts/${accountId}/d1/database/${databaseId}`;

async function d1Query(
  sql: string,
  params: unknown[] = []
): Promise<D1Result> {
  const result = await cfFetch<D1ResultSet[]>(`${d1Base}/query`, {
    method: 'POST',
    body: JSON.stringify({ sql, params }),
  });

  // The API wraps results in an array; return the first (and only) result set.
  if (!Array.isArray(result) || result.length === 0) {
    throw new Error('D1 query returned an unexpected empty result array');
  }

  return result[0];
}

// ─── ETL helpers (local dev / scripts only, not used by runtime server) ───────

/**
 * Escapes a SQL value for safe inline embedding in a SQL file.
 * Strings: wrap in single quotes, escape embedded single quotes by doubling.
 * Numbers/booleans: emit as literal.
 * null: emit NULL.
 */
function sqlLiteral(value: unknown): string {
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
      `sqlLiteral does not accept objects; got ${Object.prototype.toString.call(value)}`
    );
  }
  // string (and fallback for anything else)
  return `'${String(value).replace(/'/g, "''")}'`;
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
const ROWS_PER_INSERT = 200;

function buildSqlFile(
  statements: Array<{ sql: string; params?: unknown[] }>
): string {
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
          next.sql.slice(0, nextValuesIdx + 'VALUES'.length).trimEnd() ===
            prefix
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
        const tuples = chunk
          .map((params) => `(${params.map(sqlLiteral).join(', ')})`)
          .join(', ');
        lines.push(`${prefix} ${tuples};`);
      }
    } else {
      lines.push(inlineParams(stmt.sql, stmt.params ?? []) + ';');
      i++;
    }
  }

  return lines.join('\n') + '\n';
}

/**
 * Replaces positional '?' placeholders in a SQL string with inlined literals.
 */
function inlineParams(sql: string, params: unknown[]): string {
  let paramIndex = 0;
  const result = sql.replace(/\?/g, () => {
    if (paramIndex >= params.length) {
      throw new Error(
        `SQL has more '?' placeholders than params: ${sql}`
      );
    }
    return sqlLiteral(params[paramIndex++]);
  });
  if (paramIndex < params.length) {
    process.stderr.write(
      `[cloudflare] inlineParams: ${params.length - paramIndex} extra param(s) unused for SQL: ${sql}\n`
    );
  }
  return result;
}

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
    execSync(
      `npx wrangler d1 execute ${d1DatabaseName} --remote --file="${tmpFile}"`,
      { stdio: 'inherit' }
    );
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
async function d1Batch(
  statements: Array<{ sql: string; params?: unknown[] }>
): Promise<void> {
  if (statements.length === 0) return;

  const sql = buildSqlFile(statements);
  await d1BatchFile(sql);
}

export const d1 = {
  query: d1Query,
  batch: d1Batch,
  batchFile: d1BatchFile,
};

// ─── Vectorize client ─────────────────────────────────────────────────────────

const vectorizeBase = `${BASE}/accounts/${accountId}/vectorize/v2/indexes/${indexName}`;

async function vectorizeQuery(
  vector: number[],
  options?: { topK?: number; filter?: Record<string, string> }
): Promise<VectorizeMatch[]> {
  const body: Record<string, unknown> = { vector };
  if (options?.topK !== undefined) body['top_k'] = options.topK;
  if (options?.filter !== undefined) body['filter'] = options.filter;

  const result = await cfFetch<{ matches: VectorizeMatch[] }>(
    `${vectorizeBase}/query`,
    { method: 'POST', body: JSON.stringify(body) }
  );

  return result.matches;
}

async function vectorizeUpsert(
  vectors: Array<{
    id: string;
    values: number[];
    metadata?: Record<string, unknown>;
  }>
): Promise<void> {
  if (vectors.length === 0) return;
  if (vectors.length > 1000) {
    throw new Error(
      `Vectorize upsert accepts at most 1000 vectors; received ${vectors.length}`
    );
  }

  await cfFetch<unknown>(`${vectorizeBase}/upsert`, {
    method: 'POST',
    body: JSON.stringify({ vectors }),
  });
}

async function vectorizeDeleteByIds(ids: string[]): Promise<void> {
  if (ids.length === 0) return;

  await cfFetch<unknown>(`${vectorizeBase}/delete-by-ids`, {
    method: 'POST',
    body: JSON.stringify({ ids }),
  });
}

export const vectorize = {
  query: vectorizeQuery,
  upsert: vectorizeUpsert,
  deleteByIds: vectorizeDeleteByIds,
};

// ─── Workers AI client ────────────────────────────────────────────────────────

const DEFAULT_EMBED_MODEL = '@cf/baai/bge-base-en-v1.5';

async function workersAiEmbed(
  texts: string[],
  model: string = DEFAULT_EMBED_MODEL
): Promise<number[][]> {
  if (texts.length === 0) return [];
  if (texts.length > 100) {
    throw new Error(
      `Workers AI embed accepts at most 100 texts per request; received ${texts.length}`
    );
  }

  const result = await cfFetch<{ data: number[][] }>(
    `${BASE}/accounts/${accountId}/ai/run/${model}`,
    {
      method: 'POST',
      body: JSON.stringify({ text: texts }),
    }
  );

  return result.data;
}

export const workersAi = {
  embed: workersAiEmbed,
};
