// Tool: topical_search
//
// Combines Nave's Topical Bible (curated editorial index) with Vectorize
// semantic search to provide comprehensive topic-based Bible discovery.
//
// Flow:
//   1. Concurrently: D1 Nave's query + Vectorize embedding query
//   2. Fetch verse text for each result set from D1
//   3. Deduplicate — Nave's entries take precedence (they carry editorial notes)
//   4. Return combined results with source attribution

import type { ToolHandler } from '@mctx-ai/mcp-server';
import { T } from '@mctx-ai/mcp-server';
import { d1, vectorize, workersAi } from '../lib/cloudflare.js';
import type { Citation } from '../lib/bible-utils.js';
import { getTranslation, ensureInitialized } from '../lib/bible-utils.js';

// ─── Types ───────────────────────────────────────────────────────────────────

interface TopicalResult {
  text: string;
  citation: Citation;
  source: 'naves' | 'semantic' | 'both';
  note?: string;
}

interface TopicalSearchResponse {
  topic: string;
  results: TopicalResult[];
  total_results: number;
}

// ─── D1 row shapes ────────────────────────────────────────────────────────────

interface NaveVerseRow {
  book_name: string;
  chapter: number;
  verse: number;
  translation_abbrev: string;
  text: string;
  note: string | null;
}

interface VerseRow {
  book_id: number;
  chapter: number;
  verse: number;
  book_name: string;
  translation_abbrev: string;
  text: string;
}

// ─── Nave's D1 search ─────────────────────────────────────────────────────────

async function searchNaves(
  topic: string,
  limit: number,
): Promise<Map<string, TopicalResult>> {
  // Normalize the topic the same way the ETL does: lowercase.
  const normalized = topic.toLowerCase();

  // Escape SQLite LIKE metacharacters to prevent callers from using % or _
  // to enumerate all topics.
  const escaped = normalized.replace(/%/g, '\\%').replace(/_/g, '\\_');

  // Resolve translation ID from cache; fall back to JOIN on abbreviation if
  // the cache is not yet populated (e.g. missing env vars at startup).
  const kjvTranslation = getTranslation('KJV');
  const translationFilter = kjvTranslation
    ? `AND v.translation_id = ?`
    : `AND t.abbreviation = 'KJV'`;
  const translationParams: unknown[] = kjvTranslation ? [kjvTranslation.id] : [];

  // Use LIKE with % wildcards for partial matching so 'forgive' matches
  // 'forgiveness', 'forgiven', etc. Also try exact match first via UNION.
  const result = await d1.query(
    `SELECT
       b.name           AS book_name,
       ntv.chapter      AS chapter,
       ntv.verse        AS verse,
       t.abbreviation   AS translation_abbrev,
       v.text           AS text,
       ntv.note         AS note
     FROM nave_topic_verses ntv
     JOIN nave_topics nt  ON nt.id = ntv.topic_id
     JOIN books b         ON b.id  = ntv.book_id
     JOIN verses v        ON v.book_id       = ntv.book_id
                         AND v.chapter       = ntv.chapter
                         AND v.verse         = ntv.verse
                         ${translationFilter}
     JOIN translations t  ON t.id = v.translation_id
     WHERE nt.normalized_topic LIKE ? ESCAPE '\\'
     LIMIT ?`,
    [...translationParams, `%${escaped}%`, limit],
  );

  const resultMap = new Map<string, TopicalResult>();

  for (const row of result.results) {
    const r = row as unknown as NaveVerseRow;
    const key = `${r.book_name}:${r.chapter}:${r.verse}`;

    const citation: Citation = {
      book: r.book_name,
      chapter: r.chapter,
      verse: r.verse,
      translation: r.translation_abbrev,
    };

    const entry: TopicalResult = {
      text: r.text,
      citation,
      source: 'naves',
    };

    if (r.note) {
      entry.note = r.note;
    }

    resultMap.set(key, entry);
  }

  return resultMap;
}

// ─── Vectorize semantic search ─────────────────────────────────────────────────

async function searchSemantic(
  topic: string,
  limit: number,
): Promise<Map<string, { book_id: number; chapter: number; verse: number; translation_id: number }>> {
  // Generate embedding for the topic string.
  const embeddings = await workersAi.embed([topic]);
  if (embeddings.length === 0) return new Map();

  const matches = await vectorize.query(embeddings[0], { topK: limit });
  if (matches.length === 0) return new Map();

  // Each match metadata contains book_id, chapter, verse, translation_id.
  const coordMap = new Map<string, { book_id: number; chapter: number; verse: number; translation_id: number }>();

  for (const match of matches) {
    const meta = match.metadata;
    if (!meta) continue;

    const bookId = meta['book_id'] as number;
    const chapter = meta['chapter'] as number;
    const verse = meta['verse'] as number;
    const translationId = meta['translation_id'] as number;

    if (!bookId || !chapter || !verse || !translationId) continue;

    // Use book_id:chapter:verse as a translation-agnostic dedup key.
    // We'll fetch the actual book name via D1 query below.
    const key = `${bookId}:${chapter}:${verse}`;
    if (!coordMap.has(key)) {
      coordMap.set(key, { book_id: bookId, chapter, verse, translation_id: translationId });
    }
  }

  return coordMap;
}

// ─── Fetch verse text for semantic results ─────────────────────────────────────

// D1 limits bound parameters to 100 per statement. Each verse lookup requires
// 4 parameters (book_id, chapter, verse, translation_id), so cap chunk size at 25.
const VERSE_CHUNK_SIZE = 25;

const VERSE_SELECT_SQL = `SELECT
       v.book_id         AS book_id,
       v.chapter         AS chapter,
       v.verse           AS verse,
       b.name            AS book_name,
       t.abbreviation    AS translation_abbrev,
       v.text            AS text
     FROM verses v
     JOIN books b       ON b.id = v.book_id
     JOIN translations t ON t.id = v.translation_id
     WHERE `;

function buildVerseChunkStatement(
  chunk: Array<{ book_id: number; chapter: number; verse: number; translation_id: number }>,
): { sql: string; params: unknown[] } {
  const clauses = chunk
    .map(() => '(v.book_id = ? AND v.chapter = ? AND v.verse = ? AND v.translation_id = ?)')
    .join(' OR ');

  const params: unknown[] = [];
  for (const c of chunk) {
    params.push(c.book_id, c.chapter, c.verse, c.translation_id);
  }

  return { sql: `${VERSE_SELECT_SQL}${clauses}`, params };
}

async function fetchVerseTexts(
  coords: Array<{ book_id: number; chapter: number; verse: number; translation_id: number }>,
): Promise<Map<string, VerseRow>> {
  if (coords.length === 0) return new Map();

  // Chunk coords to stay within D1's 100-parameter-per-statement ceiling.
  const chunks: Array<typeof coords> = [];
  for (let i = 0; i < coords.length; i += VERSE_CHUNK_SIZE) {
    chunks.push(coords.slice(i, i + VERSE_CHUNK_SIZE));
  }

  // Use d1.batch() for multiple chunks to minimise HTTP round-trips (single call).
  // For a single chunk, d1.batch() still works correctly.
  const statements = chunks.map(buildVerseChunkStatement);
  const resultSets = await d1.batch(statements);

  const verseMap = new Map<string, VerseRow>();

  for (const resultSet of resultSets) {
    for (const row of resultSet.results) {
      const r = row as unknown as VerseRow;
      // Key uses book_id (numeric) to match the coordMap keys from semantic search.
      const key = `${r.book_id}:${r.chapter}:${r.verse}`;
      verseMap.set(key, r);
    }
  }

  return verseMap;
}

// ─── Tool handler ─────────────────────────────────────────────────────────────

const topicalSearch: ToolHandler = async (args, _ask?) => {
  await ensureInitialized();

  const { topic, limit } = args as { topic: string; limit: number };

  // Run Nave's D1 query and Vectorize semantic search concurrently.
  // Chain fetchVerseTexts off the semantic search promise so it starts as soon
  // as semantic results arrive, without waiting for the Naves D1 query to finish.
  const semanticCoordsPromise = searchSemantic(topic, limit);
  const semanticVerseMapPromise = semanticCoordsPromise.then((coords) =>
    fetchVerseTexts(Array.from(coords.values()))
  );

  const [navesResults, semanticCoords, semanticVerseMap] = await Promise.all([
    searchNaves(topic, limit),
    semanticCoordsPromise,
    semanticVerseMapPromise,
  ]);

  // Build final deduplicated results map.
  // Nave's results already use "book_name:chapter:verse" keys.
  // Semantic results use "book_id:chapter:verse" keys initially.
  // We need a unified key. After fetching verse text, semantic results
  // will have book_name — use that for the unified dedup key.
  const unified = new Map<string, TopicalResult>(navesResults);

  for (const [coordKey] of semanticCoords) {
    const verseRow = semanticVerseMap.get(coordKey);
    if (!verseRow) continue;

    const unifiedKey = `${verseRow.book_name}:${verseRow.chapter}:${verseRow.verse}`;

    if (unified.has(unifiedKey)) {
      // Already in results from Nave's — mark as 'both', preserve Nave's note.
      const existing = unified.get(unifiedKey)!;
      existing.source = 'both';
    } else {
      const citation: Citation = {
        book: verseRow.book_name,
        chapter: verseRow.chapter,
        verse: verseRow.verse,
        translation: verseRow.translation_abbrev,
      };

      unified.set(unifiedKey, {
        text: verseRow.text,
        citation,
        source: 'semantic',
      });
    }
  }

  const results = Array.from(unified.values()).slice(0, limit);

  const response: TopicalSearchResponse = {
    topic,
    results,
    total_results: results.length,
  };

  return response;
};

topicalSearch.description =
  'Find Bible verses on a theological topic using Nave\'s curated Topical Bible index combined with AI semantic search. ' +
  'Nave\'s index covers 5,319 theological topics. ' +
  'Best for established theological topics (forgiveness, prayer, faith, love, salvation) where Nave\'s editorial curation adds depth. ' +
  'Results are deduplicated and marked by source (naves, semantic, or both); Nave\'s results may include editorial notes. ' +
  'For open-ended or personal experience queries (e.g., "passages that helped me through grief"), prefer search_bible instead.';

topicalSearch.input = {
  topic: T.string({
    required: true,
    description:
      'The topic to search for (e.g., "forgiveness", "prayer", "faith", "love"). ' +
      'Nave\'s index is searched by keyword match; semantic search finds conceptually related verses.',
    minLength: 1,
  }),
  limit: T.number({
    required: false,
    description: 'Maximum number of results to return (default 20, max 50).',
    min: 1,
    max: 50,
    default: 20,
  }),
};

export default topicalSearch;
