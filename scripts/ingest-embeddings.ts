#!/usr/bin/env tsx
/**
 * ingest-embeddings.ts
 *
 * Generates and loads verse embeddings into Cloudflare Vectorize for all 5 translations.
 *
 * Embedding model : @cf/baai/bge-base-en-v1.5 (768 dimensions)
 * Pooling         : cls (explicit — never rely on API defaults)
 * Distance metric : cosine
 * Vector ID format: {translation}-{book_abbrev}-{chapter}-{verse}
 *                   e.g. kjv-gen-1-1, web-rev-22-21
 *
 * Modes:
 *   Full re-index (default):
 *     Deletes existing Vectorize index, creates fresh index with correct
 *     dimensions + metric, creates metadata property indexes for book_id and
 *     testament BEFORE any upserts, then embeds and upserts all ~155K verses.
 *
 *   Resume (--resume flag):
 *     Skips index management. Probes a sample of vectors to determine the
 *     last successfully ingested translation and resumes from there.
 *
 * Usage:
 *   npx tsx scripts/ingest-embeddings.ts          # full re-index
 *   npx tsx scripts/ingest-embeddings.ts --resume # resume from checkpoint
 *   npm run search:embeddings
 *
 * Required environment variables:
 *   CLOUDFLARE_API_TOKEN
 *   CLOUDFLARE_ACCOUNT_ID
 *   D1_DATABASE_ID
 *   VECTORIZE_INDEX_NAME
 */

import './load-env.js';
import { d1, workersAi } from '../src/lib/cloudflare.js';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const EMBED_BATCH_SIZE = 100;   // texts per Workers AI call
const UPSERT_BATCH_SIZE = 1000; // vectors per Vectorize upsert call
const PROGRESS_INTERVAL = 1000; // log a progress line every N vectors upserted
const EMBED_MODEL = '@cf/baai/bge-base-en-v1.5';
const DIMENSIONS = 768;
const METRIC = 'cosine';

// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------

const apiToken = process.env.CLOUDFLARE_API_TOKEN ?? '';
const accountId = process.env.CLOUDFLARE_ACCOUNT_ID ?? '';
const indexName = process.env.VECTORIZE_INDEX_NAME ?? '';

const BASE = 'https://api.cloudflare.com/client/v4';

// ---------------------------------------------------------------------------
// Translation metadata (must match etl-bible-text.ts)
// ---------------------------------------------------------------------------

interface TranslationMeta {
  id: number;
  abbreviation: string;
}

const TRANSLATIONS: TranslationMeta[] = [
  { id: 1, abbreviation: 'KJV' },
  { id: 2, abbreviation: 'WEB' },
  { id: 3, abbreviation: 'ASV' },
  { id: 4, abbreviation: 'YLT' },
  { id: 5, abbreviation: 'DBY' },
  { id: 6, abbreviation: 'NET' },
];

// ---------------------------------------------------------------------------
// Verse row returned from D1
// ---------------------------------------------------------------------------

interface VerseRow {
  id: number;
  book_id: number;
  chapter: number;
  verse: number;
  translation_id: number;
  text: string;
  book_abbrev: string;
  testament: string;
  translation_abbrev: string;
}

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

function log(msg: string): void {
  console.log(`[embed] ${msg}`);
}

// ---------------------------------------------------------------------------
// Retry with exponential backoff
// ---------------------------------------------------------------------------

async function withRetry<T>(
  fn: () => Promise<T>,
  label: string,
  maxAttempts = 4,
  baseDelayMs = 1000,
): Promise<T> {
  let lastError: unknown;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastError = err;
      if (attempt === maxAttempts) break;
      const delayMs = baseDelayMs * Math.pow(2, attempt - 1) + Math.random() * 200;
      log(`  [retry] ${label} attempt ${attempt} failed — retrying in ${Math.round(delayMs)}ms`);
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
  throw lastError;
}

// ---------------------------------------------------------------------------
// Vectorize management API helpers
// (Control-plane operations not covered by the cloudflare.ts data-plane client)
// ---------------------------------------------------------------------------

async function cfManagement<T>(path: string, method: string, body?: unknown): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${apiToken}`,
      'Content-Type': 'application/json',
    },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });

  const text = await res.text();

  // 404 on DELETE means the index does not exist — treat as success
  if (!res.ok && !(method === 'DELETE' && res.status === 404)) {
    throw new Error(`Vectorize management API ${res.status} ${res.statusText}: ${text}`);
  }

  if (!text) return undefined as T;

  const json = JSON.parse(text) as {
    success: boolean;
    errors: Array<{ code: number; message: string }>;
    result: T;
  };

  if (!json.success && !(method === 'DELETE' && res.status === 404)) {
    const msg = (json.errors ?? []).map((e) => `[${e.code}] ${e.message}`).join('; ');
    throw new Error(`Vectorize management returned success=false: ${msg}`);
  }

  return json.result;
}

async function deleteIndex(): Promise<void> {
  log(`Deleting existing index "${indexName}"...`);
  await cfManagement<unknown>(
    `/accounts/${accountId}/vectorize/v2/indexes/${indexName}`,
    'DELETE'
  );
  log('  Deleted (or did not exist)');
}

async function createIndex(): Promise<void> {
  log(`Creating index "${indexName}" (${DIMENSIONS} dims, ${METRIC})...`);
  await cfManagement<unknown>(
    `/accounts/${accountId}/vectorize/v2/indexes`,
    'POST',
    {
      name: indexName,
      config: {
        dimensions: DIMENSIONS,
        metric: METRIC,
      },
    }
  );
  log('  Created');
}

async function createMetadataIndex(propertyName: string, indexType: string): Promise<void> {
  log(`Creating metadata index on "${propertyName}" (${indexType})...`);
  await cfManagement<unknown>(
    `/accounts/${accountId}/vectorize/v2/indexes/${indexName}/metadata_index/create`,
    'POST',
    { propertyName, indexType }
  );
  log(`  Created metadata index on "${propertyName}"`);
}

// ---------------------------------------------------------------------------
// Vectorize data-plane upsert
// (Uses fetch directly so we can keep this script self-contained from the
//  cloudflare.ts module while still respecting its 1000-vector limit pattern)
// ---------------------------------------------------------------------------

interface VectorRecord {
  id: string;
  values: number[];
  metadata: {
    book_id: number;
    chapter: number;
    verse: number;
    translation_id: number;
    testament: string;
  };
}

async function upsertVectors(vectors: VectorRecord[]): Promise<void> {
  if (vectors.length === 0) return;

  const res = await fetch(
    `${BASE}/accounts/${accountId}/vectorize/v2/indexes/${indexName}/upsert`,
    {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${apiToken}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ vectors }),
    }
  );

  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`Vectorize upsert failed ${res.status} ${res.statusText}: ${body}`);
  }

  const json = (await res.json()) as { success: boolean; errors?: Array<{ code: number; message: string }> };
  if (!json.success) {
    const msg = (json.errors ?? []).map((e) => `[${e.code}] ${e.message}`).join('; ');
    throw new Error(`Vectorize upsert returned success=false: ${msg}`);
  }
}

// ---------------------------------------------------------------------------
// Resume: probe whether vectors for a translation already exist
// ---------------------------------------------------------------------------

async function translationHasVectors(translationAbbrev: string): Promise<boolean> {
  // Probe Genesis 1:1 for this translation — if it exists, the translation
  // was fully or partially ingested. The resume logic will re-upsert from the
  // beginning of the first missing translation anyway (idempotent by vector ID).
  const probeId = `${translationAbbrev.toLowerCase()}-gen-1-1`;

  const res = await fetch(
    `${BASE}/accounts/${accountId}/vectorize/v2/indexes/${indexName}/get-by-ids`,
    {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${apiToken}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ ids: [probeId] }),
    }
  );

  if (!res.ok) return false;

  const json = (await res.json()) as { success: boolean; result?: { vectors?: unknown[] } };
  if (!json.success) return false;

  return (json.result?.vectors ?? []).length > 0;
}

// ---------------------------------------------------------------------------
// Fetch all verses for a translation from D1
// ---------------------------------------------------------------------------

async function fetchVerses(translationId: number): Promise<VerseRow[]> {
  const result = await d1.query(
    `SELECT v.id, v.book_id, v.chapter, v.verse, v.translation_id, v.text,
            b.abbreviation AS book_abbrev, b.testament,
            t.abbreviation AS translation_abbrev
     FROM verses v
     JOIN books b ON b.id = v.book_id
     JOIN translations t ON t.id = v.translation_id
     WHERE v.translation_id = ?
     ORDER BY v.book_id, v.chapter, v.verse`,
    [translationId]
  );

  return result.results as unknown as VerseRow[];
}

// ---------------------------------------------------------------------------
// Ingest one translation
// ---------------------------------------------------------------------------

async function ingestTranslation(translation: TranslationMeta): Promise<void> {
  log(`Fetching verses for ${translation.abbreviation}...`);
  const verses = await fetchVerses(translation.id);
  log(`  ${verses.length.toLocaleString()} verses to embed`);

  const abbrevLower = translation.abbreviation.toLowerCase();
  let totalUpserted = 0;
  let pendingVectors: VectorRecord[] = [];

  // Process in embedding batches, accumulate into upsert batches
  for (let i = 0; i < verses.length; i += EMBED_BATCH_SIZE) {
    const batch = verses.slice(i, i + EMBED_BATCH_SIZE);
    const texts = batch.map((v) => v.text);

    const embeddings = await withRetry(
      () => workersAi.embed(texts, EMBED_MODEL),
      `embed batch ${i / EMBED_BATCH_SIZE + 1}`,
    );

    for (let j = 0; j < batch.length; j++) {
      const v = batch[j];
      const vectorId = `${abbrevLower}-${v.book_abbrev.toLowerCase()}-${v.chapter}-${v.verse}`;

      pendingVectors.push({
        id: vectorId,
        values: embeddings[j],
        metadata: {
          book_id: v.book_id,
          chapter: v.chapter,
          verse: v.verse,
          translation_id: v.translation_id,
          testament: v.testament,
        },
      });

      // Flush when upsert batch is full
      if (pendingVectors.length >= UPSERT_BATCH_SIZE) {
        await withRetry(() => upsertVectors(pendingVectors), `upsert batch at offset ${i}`);
        totalUpserted += pendingVectors.length;
        pendingVectors = [];

        if (totalUpserted % PROGRESS_INTERVAL === 0 || totalUpserted === verses.length) {
          const pct = Math.round((totalUpserted / verses.length) * 100);
          log(`  [${translation.abbreviation}] ${totalUpserted.toLocaleString()}/${verses.length.toLocaleString()} (${pct}%)`);
        }
      }
    }
  }

  // Flush remainder
  if (pendingVectors.length > 0) {
    await withRetry(() => upsertVectors(pendingVectors), `upsert final batch`);
    totalUpserted += pendingVectors.length;
  }

  log(`  [${translation.abbreviation}] Done — ${totalUpserted.toLocaleString()} vectors upserted`);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  const isResume = process.argv.includes('--resume');

  console.log('Bible Study MCP Server — Embedding Ingestion');
  console.log(`Mode: ${isResume ? 'resume' : 'full re-index'}`);
  console.log('=======================================\n');

  const required = [
    'CLOUDFLARE_API_TOKEN',
    'CLOUDFLARE_ACCOUNT_ID',
    'D1_DATABASE_ID',
    'VECTORIZE_INDEX_NAME',
  ];
  const missing = required.filter((k) => !process.env[k]);
  if (missing.length > 0) {
    console.error(`ERROR: Missing required environment variables: ${missing.join(', ')}`);
    process.exit(1);
  }

  if (!isResume) {
    // Full re-index: delete, recreate, create metadata indexes, then ingest all
    await deleteIndex();
    await createIndex();

    // Metadata indexes must exist BEFORE upserting vectors so Vectorize can
    // index them during ingestion rather than retroactively.
    await createMetadataIndex('book_id', 'number');
    await createMetadataIndex('testament', 'string');
    await createMetadataIndex('translation_id', 'number');

    log('\nStarting full ingestion for all 5 translations...\n');
    for (const translation of TRANSLATIONS) {
      await ingestTranslation(translation);
      log('');
    }
  } else {
    // Resume mode: skip translations whose probe vector already exists
    log('Probing existing vectors to determine resume point...\n');

    let totalSkipped = 0;
    for (const translation of TRANSLATIONS) {
      const exists = await translationHasVectors(translation.abbreviation);
      if (exists) {
        log(`  [${translation.abbreviation}] Probe found existing vectors — skipping`);
        totalSkipped++;
      } else {
        log(`  [${translation.abbreviation}] No vectors found — ingesting`);
        await ingestTranslation(translation);
        log('');
      }
    }

    if (totalSkipped === TRANSLATIONS.length) {
      log('All translations already present. Nothing to do.');
    }
  }

  console.log('\n========================================');
  log('Ingestion complete.');
}

main().catch((err) => {
  console.error('[embed] Unexpected error:', err);
  process.exit(1);
});
