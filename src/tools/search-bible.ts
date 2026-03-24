// search-bible.ts
// Semantic vector search over Bible text using Workers AI embeddings + Vectorize.
//
// When no translation filter is set, this tool deduplicates results to unique
// verse locations and returns all translations for each location from D1.
// When a translation filter is set, the limit applies directly to results.

import { T } from '@mctx-ai/app';
import type { ToolHandler } from '@mctx-ai/app';
import { vectorize, workersAi, d1 } from '../lib/cloudflare.js';
import {
  resolveBook,
  getTranslation,
  makeCitation,
  ensureInitialized,
  type Citation,
  type Book,
} from '../lib/bible-utils.js';

// ─── Types ────────────────────────────────────────────────────────────────────

interface VerseResult {
  citation: Citation;
  text: string;
  score: number;
}

interface SearchResult {
  query: string;
  results: VerseResult[];
  total: number;
}

interface TranslationEntry {
  abbreviation: string;
  text: string;
}

interface GroupedVerseResult {
  citation: {
    book: string;
    chapter: number;
    verse: number;
  };
  score: number;
  translations: TranslationEntry[];
}

interface GroupedSearchResult {
  query: string;
  results: GroupedVerseResult[];
  total: number;
}

// ─── Constants ────────────────────────────────────────────────────────────────

const DEFAULT_LIMIT = 10;
const MAX_LIMIT = 20;

// When no translation filter, over-fetch from Vectorize to account for
// deduplication across translations (up to 5 translations per verse).
const VECTORIZE_OVERFETCH_MULTIPLIER = 8;

// D1 limits bound parameters to 100 per statement. Each verse location
// requires 3 parameters (book_id, chapter, verse). Cap chunk size at 30
// (30 * 3 = 90 params) to leave headroom for additional parameters like
// translation_id.
const LOCATION_CHUNK_SIZE = 30;

// ─── Helpers ─────────────────────────────────────────────────────────────────

function buildVectorizeFilter(
  bookId: number | undefined,
  testament: string | undefined
): Record<string, string | number> | undefined {
  const filter: Record<string, string | number> = {};

  if (bookId !== undefined) {
    filter['book_id'] = bookId;
  }

  if (testament !== undefined) {
    filter['testament'] = testament.toUpperCase();
  }

  return Object.keys(filter).length > 0 ? filter : undefined;
}

interface VerseMeta {
  book_id: number;
  chapter: number;
  verse: number;
  translation_id: number;
}

function parseVectorMeta(metadata: Record<string, unknown>): VerseMeta | null {
  const book_id = metadata['book_id'];
  const chapter = metadata['chapter'];
  const verse = metadata['verse'];
  const translation_id = metadata['translation_id'];

  if (
    typeof book_id !== 'number' ||
    typeof chapter !== 'number' ||
    typeof verse !== 'number' ||
    typeof translation_id !== 'number'
  ) {
    return null;
  }

  return { book_id, chapter, verse, translation_id };
}

// ─── D1 fetch helpers ─────────────────────────────────────────────────────────

interface D1VerseRow {
  book_id: number;
  chapter: number;
  verse: number;
  translation_id: number;
  text: string;
  book_name: string;
  translation_abbrev: string;
}

async function fetchVersesByTranslation(
  locations: Array<{ book_id: number; chapter: number; verse: number }>,
  translationId: number
): Promise<D1VerseRow[]> {
  if (locations.length === 0) return [];

  // Chunk locations to stay within D1's 100-parameter-per-statement limit.
  // Each location uses 3 params + 1 for translationId per chunk.
  const chunks: Array<typeof locations> = [];
  for (let i = 0; i < locations.length; i += LOCATION_CHUNK_SIZE) {
    chunks.push(locations.slice(i, i + LOCATION_CHUNK_SIZE));
  }

  const allRows: D1VerseRow[] = [];

  await Promise.all(
    chunks.map(async (chunk) => {
      // Build a single query with parameterized IN-like conditions using OR.
      // D1 doesn't support row constructors, so expand to individual conditions.
      const conditions = chunk
        .map(() => '(v.book_id = ? AND v.chapter = ? AND v.verse = ?)')
        .join(' OR ');

      const params: unknown[] = chunk.flatMap((loc) => [
        loc.book_id,
        loc.chapter,
        loc.verse,
      ]);
      params.push(translationId);

      const sql = `
        SELECT
          v.book_id,
          v.chapter,
          v.verse,
          v.translation_id,
          v.text,
          b.name AS book_name,
          t.abbreviation AS translation_abbrev
        FROM verses v
        JOIN books b ON b.id = v.book_id
        JOIN translations t ON t.id = v.translation_id
        WHERE (${conditions})
          AND v.translation_id = ?
        ORDER BY b.canonical_order, v.chapter, v.verse
      `;

      const result = await d1.query(sql, params);

      const rows = result.results.map((row) => ({
        book_id: row['book_id'] as number,
        chapter: row['chapter'] as number,
        verse: row['verse'] as number,
        translation_id: row['translation_id'] as number,
        text: row['text'] as string,
        book_name: row['book_name'] as string,
        translation_abbrev: row['translation_abbrev'] as string,
      }));

      allRows.push(...rows);
    })
  );

  return allRows;
}

async function fetchVersesByLocations(
  locations: Array<{ book_id: number; chapter: number; verse: number }>
): Promise<D1VerseRow[]> {
  if (locations.length === 0) return [];

  // Chunk locations to stay within D1's 100-parameter-per-statement limit.
  // Each location uses 3 params (book_id, chapter, verse) with no extra params.
  const chunks: Array<typeof locations> = [];
  for (let i = 0; i < locations.length; i += LOCATION_CHUNK_SIZE) {
    chunks.push(locations.slice(i, i + LOCATION_CHUNK_SIZE));
  }

  const allRows: D1VerseRow[] = [];

  await Promise.all(
    chunks.map(async (chunk) => {
      const conditions = chunk
        .map(() => '(v.book_id = ? AND v.chapter = ? AND v.verse = ?)')
        .join(' OR ');

      const params: unknown[] = chunk.flatMap((loc) => [
        loc.book_id,
        loc.chapter,
        loc.verse,
      ]);

      const sql = `
        SELECT
          v.book_id,
          v.chapter,
          v.verse,
          v.translation_id,
          v.text,
          b.name AS book_name,
          t.abbreviation AS translation_abbrev
        FROM verses v
        JOIN books b ON b.id = v.book_id
        JOIN translations t ON t.id = v.translation_id
        WHERE (${conditions})
        ORDER BY b.canonical_order, v.chapter, v.verse, t.abbreviation
      `;

      const result = await d1.query(sql, params);

      const rows = result.results.map((row) => ({
        book_id: row['book_id'] as number,
        chapter: row['chapter'] as number,
        verse: row['verse'] as number,
        translation_id: row['translation_id'] as number,
        text: row['text'] as string,
        book_name: row['book_name'] as string,
        translation_abbrev: row['translation_abbrev'] as string,
      }));

      allRows.push(...rows);
    })
  );

  return allRows;
}

// ─── Tool implementation ──────────────────────────────────────────────────────

const searchBible: ToolHandler = async (args, _ask?) => {
  await ensureInitialized();

  const {
    query,
    limit: limitRaw,
    translation: translationArg,
    book: bookArg,
    testament: testamentArg,
  } = args as {
    query: string;
    limit?: number;
    translation?: string;
    book?: string;
    testament?: string;
  };

  // Validate and clamp limit.
  const limit = Math.min(
    Math.max(1, Math.floor(limitRaw ?? DEFAULT_LIMIT)),
    MAX_LIMIT
  );

  // Resolve optional book filter.
  let bookFilter: Book | undefined;
  if (bookArg) {
    const resolved = resolveBook(bookArg);
    if (!resolved) {
      throw new Error(`Unknown book: "${bookArg}"`);
    }
    bookFilter = resolved;
  }

  // Resolve optional translation filter.
  let translationId: number | undefined;
  if (translationArg) {
    const resolved = getTranslation(translationArg);
    if (!resolved) {
      throw new Error(`Unknown translation: "${translationArg}"`);
    }
    translationId = resolved.id;
  }

  // Validate testament filter.
  let testamentFilter: string | undefined;
  if (testamentArg) {
    const upper = testamentArg.toUpperCase();
    if (upper !== 'OT' && upper !== 'NT') {
      throw new Error(`Invalid testament: "${testamentArg}". Must be "OT" or "NT".`);
    }
    testamentFilter = upper;
  }

  // Generate embedding for the query.
  const embeddings = await workersAi.embed([query]);
  if (!embeddings || embeddings.length === 0 || !embeddings[0]) {
    throw new Error('Failed to generate embedding for query');
  }
  const queryVector = embeddings[0];

  // Determine Vectorize query parameters.
  // translation_id is intentionally excluded from the Vectorize filter — Vectorize metadata
  // post-filtering is unreliable and drops too many candidates. Instead, we apply translation
  // filtering in application code after results are returned.
  const filter = buildVectorizeFilter(bookFilter?.id, testamentFilter);

  // Overfetch to ensure enough candidates survive deduplication and application-side translation
  // filtering. ANN retrieval returns candidates before any post-filtering, so we need headroom.
  const topK = Math.min(limit * VECTORIZE_OVERFETCH_MULTIPLIER, 20);

  // Query Vectorize.
  const matches = await vectorize.query(queryVector, { topK, filter });

  if (!matches || matches.length === 0) {
    const result: SearchResult = { query, results: [], total: 0 };
    return result;
  }

  if (translationId) {
    // Translation filter path: limit applies directly.
    // Post-filter matches in application code to only those belonging to the requested
    // translation. This is more reliable than Vectorize metadata filtering, which can
    // silently drop candidates during ANN retrieval.
    const translationMatches = matches.filter(
      (match) => match.metadata?.['translation_id'] === translationId
    );

    // Collect valid locations from the filtered vector matches.
    const locations: Array<{
      book_id: number;
      chapter: number;
      verse: number;
      score: number;
    }> = [];

    for (const match of translationMatches) {
      if (!match.metadata) continue;
      const meta = parseVectorMeta(match.metadata);
      if (!meta) continue;
      locations.push({
        book_id: meta.book_id,
        chapter: meta.chapter,
        verse: meta.verse,
        score: match.score,
      });
    }

    if (locations.length === 0) {
      const result: SearchResult = { query, results: [], total: 0 };
      return result;
    }

    // Build a score map keyed by location for later annotation.
    const scoreByLocation = new Map<string, number>();
    for (const loc of locations) {
      scoreByLocation.set(`${loc.book_id}:${loc.chapter}:${loc.verse}`, loc.score);
    }

    // Fetch verse text from D1.
    const rows = await fetchVersesByTranslation(locations, translationId);

    const results: VerseResult[] = rows.map((row) => {
      const score =
        scoreByLocation.get(`${row.book_id}:${row.chapter}:${row.verse}`) ?? 0;
      const book = resolveBook(row.book_name) ?? ({ id: row.book_id, name: row.book_name } as Book);
      const citation: Citation = makeCitation(
        book,
        row.chapter,
        row.verse,
        row.translation_abbrev
      );
      return { citation, text: row.text, score };
    });

    // Sort by descending score (D1 returns in canonical order).
    results.sort((a, b) => b.score - a.score);

    const output: SearchResult = {
      query,
      results,
      total: results.length,
    };
    return output;
  }

  // No translation filter path: deduplicate to unique verse locations,
  // then return all translations for each.

  // Track unique locations in score order.
  const seenLocations = new Set<string>();
  const uniqueLocations: Array<{
    book_id: number;
    chapter: number;
    verse: number;
    score: number;
  }> = [];

  for (const match of matches) {
    if (uniqueLocations.length >= limit) break;
    if (!match.metadata) continue;
    const meta = parseVectorMeta(match.metadata);
    if (!meta) continue;

    const key = `${meta.book_id}:${meta.chapter}:${meta.verse}`;
    if (seenLocations.has(key)) continue;

    seenLocations.add(key);
    uniqueLocations.push({
      book_id: meta.book_id,
      chapter: meta.chapter,
      verse: meta.verse,
      score: match.score,
    });
  }

  if (uniqueLocations.length === 0) {
    const result: SearchResult = { query, results: [], total: 0 };
    return result;
  }

  // Fetch all translations for the unique locations.
  const rows = await fetchVersesByLocations(uniqueLocations);

  // Group rows by verse location, preserving score order from uniqueLocations.
  const groupMap = new Map<string, GroupedVerseResult>();
  for (const loc of uniqueLocations) {
    const key = `${loc.book_id}:${loc.chapter}:${loc.verse}`;
    groupMap.set(key, {
      citation: { book: '', chapter: loc.chapter, verse: loc.verse },
      score: loc.score,
      translations: [],
    });
  }

  for (const row of rows) {
    const key = `${row.book_id}:${row.chapter}:${row.verse}`;
    const group = groupMap.get(key);
    if (!group) continue;

    // Populate book name from the first row for this location.
    if (!group.citation.book) {
      const book = resolveBook(row.book_name) ?? ({ id: row.book_id, name: row.book_name } as Book);
      group.citation.book = book.name;
    }

    group.translations.push({ abbreviation: row.translation_abbrev, text: row.text });
  }

  // Sort translations within each group alphabetically.
  for (const group of groupMap.values()) {
    group.translations.sort((a, b) => a.abbreviation.localeCompare(b.abbreviation));
  }

  // Collect groups in score-descending order (uniqueLocations preserves this order).
  const results: GroupedVerseResult[] = uniqueLocations
    .map((loc) => groupMap.get(`${loc.book_id}:${loc.chapter}:${loc.verse}`))
    .filter((g): g is GroupedVerseResult => g !== undefined);

  const output: GroupedSearchResult = {
    query,
    results,
    total: results.length,
  };
  return output;
};

searchBible.annotations = {
  readOnlyHint: true,
  destructiveHint: false,
  idempotentHint: true,
  openWorldHint: true,
};

searchBible.description =
  'Semantic similarity search — finds Bible verses whose meaning matches your query, even when the exact words differ. Best for exploratory discovery of related verses when the user does not know exact wording or references. ' +
  'Good for metaphor, imagery, or concept matching such as "living water", "God as shepherd", or "light as holiness". ' +
  'When no translation filter is set, results are grouped by verse location with a translations[] array containing all available translations for each location. ' +
  'Do not use as the primary tool for broad theological questions like "What does the Bible say about X?" or for themes whose main witnesses may be books, narratives, or large passages rather than isolated verses. ' +
  'For broad thematic or narrative-level retrieval, use topical_search instead. (Soft boundary: semantic_search can serve as a secondary tool after topical_search in layered workflows.) Filters: translation, book, testament (OT/NT).';

searchBible.input = {
  query: T.string({
    required: true,
    description:
      'Natural language search query (e.g., "rivers of living water", "the armor of God")',
    minLength: 1,
    maxLength: 500,
  }),
  limit: T.number({
    description: `Maximum number of unique verse locations to return (default ${DEFAULT_LIMIT}, max ${MAX_LIMIT}). When no translation filter is set, each location may include multiple translations.`,
    min: 1,
    max: MAX_LIMIT,
    default: DEFAULT_LIMIT,
  }),
  translation: T.string({
    description:
      'Filter results to a specific translation abbreviation (e.g., "KJV", "WEB", "ASV", "YLT", "DBY"). When set, limit applies to total results.',
  }),
  book: T.string({
    description:
      'Filter results to a specific book of the Bible (e.g., "Genesis", "John", "Psalms"). Accepts full names, abbreviations, and common aliases.',
  }),
  testament: T.string({
    description: 'Filter results to a testament: "OT" (Old Testament) or "NT" (New Testament).',
    enum: ['OT', 'NT'],
  }),
};

export default searchBible;
