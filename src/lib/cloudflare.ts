// Cloudflare HTTP REST API client
// Reads config from environment at module scope (nodejs_compat mode)

const apiToken =
  process.env.BIBLE_API_TOKEN ?? process.env.CLOUDFLARE_API_TOKEN ?? '';
const accountId =
  process.env.BIBLE_ACCOUNT_ID ?? process.env.CLOUDFLARE_ACCOUNT_ID ?? '';
const databaseId = process.env.D1_DATABASE_ID ?? '';
const indexName = process.env.VECTORIZE_INDEX_NAME ?? '';

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

async function d1Batch(
  statements: Array<{ sql: string; params?: unknown[] }>
): Promise<D1Result[]> {
  if (statements.length === 0) {
    return [];
  }
  if (statements.length > 100) {
    throw new Error(
      `D1 batch accepts at most 100 statements; received ${statements.length}`
    );
  }

  // The /raw endpoint accepts an array of {sql, params} objects.
  const result = await cfFetch<D1ResultSet[]>(`${d1Base}/raw`, {
    method: 'POST',
    body: JSON.stringify(
      statements.map(({ sql, params = [] }) => ({ sql, params }))
    ),
  });

  return result;
}

export const d1 = {
  query: d1Query,
  batch: d1Batch,
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

  const result = await cfFetch<{ data: number[][] }>(
    `${BASE}/accounts/${accountId}/ai/run/${model}`,
    {
      method: 'POST',
      body: JSON.stringify({
        text: texts,
        pooling: { strategy: 'cls' },
      }),
    }
  );

  return result.data;
}

export const workersAi = {
  embed: workersAiEmbed,
};
