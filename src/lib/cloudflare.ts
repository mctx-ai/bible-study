// Cloudflare HTTP REST API client
// Config is read lazily at request time — not at module evaluation time.
// In Cloudflare Workers, process.env is not populated until the request
// handler runs, so module-scope reads always return empty strings.

let _config: {
  apiToken: string;
  accountId: string;
  databaseId: string;
  indexName: string;
} | null = null;

export function getConfig() {
  if (!_config) {
    _config = {
      apiToken:
        process.env.BIBLE_API_TOKEN ?? process.env.CLOUDFLARE_API_TOKEN ?? '',
      accountId:
        process.env.BIBLE_ACCOUNT_ID ?? process.env.CLOUDFLARE_ACCOUNT_ID ?? '',
      databaseId: process.env.D1_DATABASE_ID ?? '',
      indexName: process.env.VECTORIZE_INDEX_NAME ?? '',
    };
  }
  return _config;
}

export function resetConfigForTesting() {
  _config = null;
}

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
    Authorization: `Bearer ${getConfig().apiToken}`,
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

async function d1Query(
  sql: string,
  params: unknown[] = []
): Promise<D1Result> {
  const { accountId, databaseId } = getConfig();
  const d1Base = `${BASE}/accounts/${accountId}/d1/database/${databaseId}`;
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

async function d1Batch(
  statements: Array<{ sql: string; params?: unknown[] }>
): Promise<D1Result[]> {
  if (statements.length === 0) return [];

  const body = statements.map((stmt) => ({
    sql: stmt.sql,
    params: stmt.params ?? [],
  }));

  const { accountId, databaseId } = getConfig();
  const d1Base = `${BASE}/accounts/${accountId}/d1/database/${databaseId}`;

  // The D1 batch endpoint accepts an array of SQL objects and returns
  // one result set per statement in input order — single HTTP round-trip.
  const results = await cfFetch<D1ResultSet[]>(`${d1Base}/query`, {
    method: 'POST',
    body: JSON.stringify(body),
  });

  if (!Array.isArray(results) || results.length !== statements.length) {
    throw new Error(
      `D1 batch returned ${Array.isArray(results) ? results.length : 'non-array'} results for ${statements.length} statements`
    );
  }

  return results;
}

export const d1 = {
  query: d1Query,
  batch: d1Batch,
};

// ─── Vectorize client ─────────────────────────────────────────────────────────

async function vectorizeQuery(
  vector: number[],
  options?: { topK?: number; filter?: Record<string, string> }
): Promise<VectorizeMatch[]> {
  const { accountId, indexName } = getConfig();
  const vectorizeBase = `${BASE}/accounts/${accountId}/vectorize/v2/indexes/${indexName}`;
  const body: Record<string, unknown> = { vector, returnMetadata: 'all' };
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

  const { accountId, indexName } = getConfig();
  const vectorizeBase = `${BASE}/accounts/${accountId}/vectorize/v2/indexes/${indexName}`;

  await cfFetch<unknown>(`${vectorizeBase}/upsert`, {
    method: 'POST',
    body: JSON.stringify({ vectors }),
  });
}

async function vectorizeDeleteByIds(ids: string[]): Promise<void> {
  if (ids.length === 0) return;

  const { accountId, indexName } = getConfig();
  const vectorizeBase = `${BASE}/accounts/${accountId}/vectorize/v2/indexes/${indexName}`;

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

  const { accountId } = getConfig();
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
