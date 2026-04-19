#!/usr/bin/env tsx
/**
 * ingest-topic-embeddings.ts
 *
 * Embeds enriched Nave's topic names and book-level theme summaries into the
 * bible-topics Vectorize index.
 *
 * Embedding model : @cf/baai/bge-base-en-v1.5 (768 dimensions)
 * Distance metric : cosine
 *
 * Vector ID format:
 *   topic-{topic_id}    — enriched topic string (5,319 topics)
 *   book-{book_id}      — book summary string (66 books)
 *
 * Metadata:
 *   { type: 'topic', topic_id: N, name: '<topic_name>' }
 *   { type: 'book_summary', book_id: N, name: '<book_name>' }
 *
 * Text formats:
 *   Topic: "SUFFERING: affliction, patience, trials, persecution, tribulation"
 *          (top 5 co-occurring topics by shared verse count)
 *   Book:  "Job: SUFFERING, AFFLICTION, PATIENCE, GOD PROVIDENCE, FAITH, SATAN,
 *                INTEGRITY, PRAYER, RESTORATION, RIGHTEOUSNESS"
 *          (top 10 topics by verse count for that book)
 *
 * Co-occurrence is computed in-memory: build verseKey→Set<topicId> map from
 * nave_topic_verses, then count pairwise overlaps.
 *
 * Modes:
 *   Full re-index (default):
 *     Deletes existing Vectorize index, creates fresh index with correct
 *     dimensions + metric, creates metadata property indexes BEFORE any upserts,
 *     then embeds and upserts all 5,385 texts.
 *
 *   Resume (--resume flag):
 *     Skips index management. Probes sample vectors to determine what is already
 *     ingested and skips those.
 *
 * Usage:
 *   npx tsx scripts/ingest-topic-embeddings.ts          # full re-index
 *   npx tsx scripts/ingest-topic-embeddings.ts --resume # resume from checkpoint
 *   npm run search:topic-embeddings
 *
 * Required environment variables:
 *   CLOUDFLARE_API_TOKEN
 *   CLOUDFLARE_ACCOUNT_ID
 *   D1_DATABASE_ID
 *   VECTORIZE_TOPIC_INDEX_NAME
 */

import './load-env.js';
import { d1, workersAi } from '../src/lib/cloudflare.js';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const EMBED_BATCH_SIZE = 100;   // texts per Workers AI call
const UPSERT_BATCH_SIZE = 1000; // vectors per Vectorize upsert call
const EMBED_MODEL = '@cf/baai/bge-base-en-v1.5';
const DIMENSIONS = 768;
const METRIC = 'cosine';

const TOP_COOCCURRING = 5;  // co-occurring topics to include per topic
const TOP_BOOK_TOPICS = 10; // topics to include per book summary

// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------

const apiToken = process.env.CLOUDFLARE_API_TOKEN ?? '';
const accountId = process.env.CLOUDFLARE_ACCOUNT_ID ?? '';
const indexName = process.env.VECTORIZE_TOPIC_INDEX_NAME ?? '';

const BASE = 'https://api.cloudflare.com/client/v4';

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

function log(msg: string): void {
  console.log(`[topic-embed] ${msg}`);
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
// Vectorize upsert
// ---------------------------------------------------------------------------

interface VectorRecord {
  id: string;
  values: number[];
  metadata: Record<string, unknown>;
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
// Resume: probe whether a vector already exists in the index
// ---------------------------------------------------------------------------

async function vectorExists(vectorId: string): Promise<boolean> {
  const res = await fetch(
    `${BASE}/accounts/${accountId}/vectorize/v2/indexes/${indexName}/get-by-ids`,
    {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${apiToken}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ ids: [vectorId] }),
    }
  );

  if (!res.ok) return false;

  const json = (await res.json()) as { success: boolean; result?: { vectors?: unknown[] } };
  if (!json.success) return false;

  return (json.result?.vectors ?? []).length > 0;
}

// ---------------------------------------------------------------------------
// D1 data types
// ---------------------------------------------------------------------------

interface TopicRow {
  id: number;
  topic_name: string;
}

interface TopicVerseRow {
  topic_id: number;
  book_id: number;
  chapter: number;
  verse: number;
}

interface BookRow {
  id: number;
  name: string;
}

// ---------------------------------------------------------------------------
// Fetch data from D1
// ---------------------------------------------------------------------------

async function fetchAllTopics(): Promise<TopicRow[]> {
  const result = await d1.query(
    'SELECT id, topic_name FROM nave_topics ORDER BY id',
    []
  );
  return result.results as unknown as TopicRow[];
}

async function fetchAllTopicVerses(): Promise<TopicVerseRow[]> {
  // Fetch in pages to avoid D1 REST response size limits
  const rows: TopicVerseRow[] = [];
  const pageSize = 10000;
  let offset = 0;

  while (true) {
    const result = await d1.query(
      'SELECT topic_id, book_id, chapter, verse FROM nave_topic_verses ORDER BY topic_id, book_id, chapter, verse LIMIT ? OFFSET ?',
      [pageSize, offset]
    );
    rows.push(...(result.results as unknown as TopicVerseRow[]));
    if (result.results.length < pageSize) break;
    offset += pageSize;
  }

  return rows;
}

async function fetchAllBooks(): Promise<BookRow[]> {
  const result = await d1.query(
    'SELECT id, name FROM books ORDER BY id',
    []
  );
  return result.results as unknown as BookRow[];
}

// ---------------------------------------------------------------------------
// Build enriched topic strings via co-occurrence
// ---------------------------------------------------------------------------

interface TextRecord {
  id: string;
  text: string;
  metadata: Record<string, unknown>;
}

function buildTopicTexts(
  topics: TopicRow[],
  topicVerseRows: TopicVerseRow[],
): TextRecord[] {
  // Build verseKey → Set<topicId> map for co-occurrence calculation
  const verseToTopics = new Map<string, Set<number>>();
  for (const row of topicVerseRows) {
    const verseKey = `${row.book_id}:${row.chapter}:${row.verse}`;
    let topicSet = verseToTopics.get(verseKey);
    if (!topicSet) {
      topicSet = new Set<number>();
      verseToTopics.set(verseKey, topicSet);
    }
    topicSet.add(row.topic_id);
  }

  // Build topicId → Set<verseKey> map for efficient pairwise overlap counting
  const topicToVerses = new Map<number, Set<string>>();
  for (const row of topicVerseRows) {
    const verseKey = `${row.book_id}:${row.chapter}:${row.verse}`;
    let verseSet = topicToVerses.get(row.topic_id);
    if (!verseSet) {
      verseSet = new Set<string>();
      topicToVerses.set(row.topic_id, verseSet);
    }
    verseSet.add(verseKey);
  }

  // Build topic_id → topic_name lookup
  const topicNameById = new Map<number, string>();
  for (const topic of topics) {
    topicNameById.set(topic.id, topic.topic_name);
  }

  const records: TextRecord[] = [];

  for (const topic of topics) {
    const myVerses = topicToVerses.get(topic.id);
    if (!myVerses || myVerses.size === 0) {
      // Topic with no verse associations — embed the name alone
      records.push({
        id: `topic-${topic.id}`,
        text: topic.topic_name,
        metadata: { type: 'topic', topic_id: topic.id, name: topic.topic_name },
      });
      continue;
    }

    // Count pairwise overlap: for each verse this topic has, count how many
    // other topics also have that verse
    const overlapCount = new Map<number, number>();
    for (const verseKey of myVerses) {
      const coTopics = verseToTopics.get(verseKey);
      if (!coTopics) continue;
      for (const coTopicId of coTopics) {
        if (coTopicId === topic.id) continue;
        overlapCount.set(coTopicId, (overlapCount.get(coTopicId) ?? 0) + 1);
      }
    }

    // Sort by overlap count descending, take top N
    const topCooccurring = Array.from(overlapCount.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, TOP_COOCCURRING)
      .map(([id]) => topicNameById.get(id) ?? '')
      .filter(Boolean);

    const text = topCooccurring.length > 0
      ? `${topic.topic_name}: ${topCooccurring.join(', ')}`
      : topic.topic_name;

    records.push({
      id: `topic-${topic.id}`,
      text,
      metadata: { type: 'topic', topic_id: topic.id, name: topic.topic_name },
    });
  }

  return records;
}

// ---------------------------------------------------------------------------
// Build book summary strings
// ---------------------------------------------------------------------------

function buildBookTexts(
  books: BookRow[],
  topics: TopicRow[],
  topicVerseRows: TopicVerseRow[],
): TextRecord[] {
  // Count verses per (book_id, topic_id) pair
  const bookTopicCount = new Map<string, number>();
  for (const row of topicVerseRows) {
    const key = `${row.book_id}:${row.topic_id}`;
    bookTopicCount.set(key, (bookTopicCount.get(key) ?? 0) + 1);
  }

  const topicNameById = new Map<number, string>();
  for (const topic of topics) {
    topicNameById.set(topic.id, topic.topic_name);
  }

  const records: TextRecord[] = [];

  for (const book of books) {
    // Collect (topic_id, count) pairs for this book
    const topicCounts: Array<{ topicId: number; count: number }> = [];
    for (const [key, count] of bookTopicCount.entries()) {
      const [bookIdStr, topicIdStr] = key.split(':');
      if (parseInt(bookIdStr, 10) === book.id) {
        topicCounts.push({ topicId: parseInt(topicIdStr, 10), count });
      }
    }

    // Sort by count descending, take top N
    topicCounts.sort((a, b) => b.count - a.count);
    const topTopics = topicCounts
      .slice(0, TOP_BOOK_TOPICS)
      .map(({ topicId }) => topicNameById.get(topicId) ?? '')
      .filter(Boolean);

    const text = topTopics.length > 0
      ? `${book.name}: ${topTopics.join(', ')}`
      : book.name;

    records.push({
      id: `book-${book.id}`,
      text,
      metadata: { type: 'book_summary', book_id: book.id, name: book.name },
    });
  }

  return records;
}

// ---------------------------------------------------------------------------
// Embed and upsert all records
// ---------------------------------------------------------------------------

async function ingestRecords(records: TextRecord[], label: string): Promise<void> {
  log(`Embedding and upserting ${records.length.toLocaleString()} ${label}...`);

  let totalUpserted = 0;
  let pendingVectors: VectorRecord[] = [];

  for (let i = 0; i < records.length; i += EMBED_BATCH_SIZE) {
    const batch = records.slice(i, i + EMBED_BATCH_SIZE);
    const texts = batch.map((r) => r.text);

    const embeddings = await withRetry(
      () => workersAi.embed(texts, EMBED_MODEL),
      `embed batch ${Math.floor(i / EMBED_BATCH_SIZE) + 1}`,
    );

    for (let j = 0; j < batch.length; j++) {
      pendingVectors.push({
        id: batch[j].id,
        values: embeddings[j],
        metadata: batch[j].metadata,
      });

      if (pendingVectors.length >= UPSERT_BATCH_SIZE) {
        await withRetry(() => upsertVectors(pendingVectors), `upsert batch at offset ${i}`);
        totalUpserted += pendingVectors.length;
        pendingVectors = [];
        log(`  [${label}] ${totalUpserted.toLocaleString()}/${records.length.toLocaleString()} upserted`);
      }
    }
  }

  // Flush remainder
  if (pendingVectors.length > 0) {
    await withRetry(() => upsertVectors(pendingVectors), 'upsert final batch');
    totalUpserted += pendingVectors.length;
  }

  log(`  [${label}] Done — ${totalUpserted.toLocaleString()} vectors upserted`);
}

// ---------------------------------------------------------------------------
// Resume: ingest only records whose vectors don't yet exist
// ---------------------------------------------------------------------------

async function ingestRecordsResume(records: TextRecord[], label: string): Promise<void> {
  log(`Resume mode: probing ${label} vectors...`);

  // Probe the first record as a representative sample
  if (records.length === 0) return;

  const probeId = records[0].id;
  const exists = await vectorExists(probeId);

  if (exists) {
    log(`  [${label}] Probe vector "${probeId}" found — skipping (already ingested)`);
    return;
  }

  log(`  [${label}] Probe vector "${probeId}" not found — ingesting`);
  await ingestRecords(records, label);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  const isResume = process.argv.includes('--resume');

  console.log('Bible Study MCP Server — Topic Embedding Ingestion');
  console.log(`Mode: ${isResume ? 'resume' : 'full re-index'}`);
  console.log('============================================\n');

  const required = [
    'CLOUDFLARE_API_TOKEN',
    'CLOUDFLARE_ACCOUNT_ID',
    'D1_DATABASE_ID',
    'VECTORIZE_TOPIC_INDEX_NAME',
  ];
  const missing = required.filter((k) => !process.env[k]);
  if (missing.length > 0) {
    console.error(`ERROR: Missing required environment variables: ${missing.join(', ')}`);
    process.exit(1);
  }

  // Fetch source data from D1
  log('Fetching topics from D1...');
  const topics = await fetchAllTopics();
  log(`  ${topics.length.toLocaleString()} topics loaded`);

  log('Fetching topic-verse associations from D1...');
  const topicVerseRows = await fetchAllTopicVerses();
  log(`  ${topicVerseRows.length.toLocaleString()} topic-verse rows loaded`);

  log('Fetching books from D1...');
  const books = await fetchAllBooks();
  log(`  ${books.length.toLocaleString()} books loaded\n`);

  // Build text records
  log('Building enriched topic strings (co-occurrence analysis)...');
  const topicRecords = buildTopicTexts(topics, topicVerseRows);
  log(`  Built ${topicRecords.length.toLocaleString()} topic texts\n`);

  log('Building book summary strings...');
  const bookRecords = buildBookTexts(books, topics, topicVerseRows);
  log(`  Built ${bookRecords.length.toLocaleString()} book summary texts\n`);

  const totalRecords = topicRecords.length + bookRecords.length;
  log(`Total texts to embed: ${totalRecords.toLocaleString()} (${topicRecords.length} topics + ${bookRecords.length} books)\n`);

  if (!isResume) {
    // Full re-index: delete, recreate, create metadata indexes, then ingest all
    await deleteIndex();
    await createIndex();

    // Metadata indexes must exist BEFORE upserting vectors
    await createMetadataIndex('type', 'string');
    await createMetadataIndex('topic_id', 'number');
    await createMetadataIndex('book_id', 'number');

    log('\nStarting full ingestion...\n');
    await ingestRecords(topicRecords, 'topics');
    log('');
    await ingestRecords(bookRecords, 'book summaries');
  } else {
    // Resume mode: probe and skip already-ingested batches
    await ingestRecordsResume(topicRecords, 'topics');
    log('');
    await ingestRecordsResume(bookRecords, 'book summaries');
  }

  console.log('\n=============================================');
  log('Ingestion complete.');
}

main().catch((err) => {
  console.error('[topic-embed] Unexpected error:', err);
  process.exit(1);
});
