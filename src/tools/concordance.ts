// concordance.ts — word occurrence lookup tool using FTS5 full-text search
//
// Returns every verse containing a given word, grouped by book in canonical
// order (Genesis → Revelation). Supports optional translation filter and a
// configurable result limit. When results are truncated a total_count field
// is included so callers know there are more matches.

import type { ToolHandler } from '@mctx-ai/mcp-server';
import { T } from '@mctx-ai/mcp-server';
import { d1 } from '../lib/cloudflare.js';
import {
  getTranslation,
  isValidTranslation,
  makeCitation,
  resolveBook,
  ensureInitialized,
} from '../lib/bible-utils.js';
import type { Citation } from '../lib/bible-utils.js';
import { sanitizeFts5 } from './find-text.js';

// ─── concordance tool ─────────────────────────────────────────────────────────

interface BookOccurrences {
  book: string;
  count: number;
  verses: Array<{
    citation: Citation;
    text: string;
  }>;
}

interface ConcordanceResult {
  word: string;
  translation?: string;
  limit: number;
  total_count?: number;
  truncated: boolean;
  occurrences: BookOccurrences[];
}

const CONCORDANCE_DEFAULT_LIMIT = 100;
const CONCORDANCE_MAX_LIMIT = 500;

const concordance: ToolHandler = async (args, _ask?) => {
  await ensureInitialized();

  const { word, translation, limit: rawLimit } = args as {
    word: string;
    translation?: string;
    limit?: number;
  };

  const limit = Math.max(1, Math.floor(Math.min(rawLimit ?? CONCORDANCE_DEFAULT_LIMIT, CONCORDANCE_MAX_LIMIT)));

  // Validate translation filter if provided.
  if (translation !== undefined && !isValidTranslation(translation)) {
    throw new Error(
      `Unknown translation "${translation}". Use the bible://translations resource to list available translations.`
    );
  }

  const ftsPhrase = sanitizeFts5(word);

  // Fetch one extra row beyond the limit so we can detect truncation without
  // relying solely on the count query result.
  const fetchLimit = limit + 1;

  let resultsSql: string;
  let resultsParams: unknown[];
  let countSql: string;
  let countParams: unknown[];

  if (translation !== undefined) {
    const t = getTranslation(translation);
    resultsSql = `
      SELECT v.id, v.chapter, v.verse, v.text, b.name AS book_name, b.canonical_order, t.abbreviation AS translation_abbrev
      FROM verses_fts f
      JOIN verses v ON f.rowid = v.id
      JOIN books b ON v.book_id = b.id
      JOIN translations t ON v.translation_id = t.id
      WHERE verses_fts MATCH ?
        AND v.translation_id = ?
      ORDER BY b.canonical_order, v.chapter, v.verse
      LIMIT ?
    `.trim();
    resultsParams = [ftsPhrase, t!.id, fetchLimit];
    countSql = `
      SELECT COUNT(*) AS total
      FROM verses_fts f
      JOIN verses v ON f.rowid = v.id
      WHERE verses_fts MATCH ?
        AND v.translation_id = ?
    `.trim();
    countParams = [ftsPhrase, t!.id];
  } else {
    resultsSql = `
      SELECT v.id, v.chapter, v.verse, v.text, b.name AS book_name, b.canonical_order, t.abbreviation AS translation_abbrev
      FROM verses_fts f
      JOIN verses v ON f.rowid = v.id
      JOIN books b ON v.book_id = b.id
      JOIN translations t ON v.translation_id = t.id
      WHERE verses_fts MATCH ?
      ORDER BY b.canonical_order, v.chapter, v.verse
      LIMIT ?
    `.trim();
    resultsParams = [ftsPhrase, fetchLimit];
    countSql = `
      SELECT COUNT(*) AS total
      FROM verses_fts f
      JOIN verses v ON f.rowid = v.id
      WHERE verses_fts MATCH ?
    `.trim();
    countParams = [ftsPhrase];
  }

  // Issue results and count queries concurrently.
  // The count result is only surfaced to callers when truncation occurs, but
  // fetching upfront avoids a serial round-trip in the truncated case.
  const [result, countResult] = await Promise.all([
    d1.query(resultsSql, resultsParams),
    d1.query(countSql, countParams),
  ]);

  const truncated = result.results.length > limit;
  const rows = truncated ? result.results.slice(0, limit) : result.results;

  // Group by book in canonical order. The SQL ORDER BY canonical_order already
  // guarantees rows arrive in the right sequence — we just collect them.
  const bookMap = new Map<string, BookOccurrences>();
  const bookOrder: string[] = [];

  for (const row of rows) {
    const bookName = row['book_name'] as string;
    const chapter = row['chapter'] as number;
    const verse = row['verse'] as number;
    const translationAbbrev = row['translation_abbrev'] as string;
    const text = row['text'] as string;

    const book = resolveBook(bookName);
    const citation: Citation =
      book !== null
        ? makeCitation(book, chapter, verse, translationAbbrev)
        : {
            book: bookName,
            chapter,
            verse,
            translation: translationAbbrev,
          };

    if (!bookMap.has(bookName)) {
      bookMap.set(bookName, { book: bookName, count: 0, verses: [] });
      bookOrder.push(bookName);
    }

    const entry = bookMap.get(bookName)!;
    entry.count += 1;
    entry.verses.push({ citation, text });
  }

  const occurrences = bookOrder.map((name) => bookMap.get(name)!);

  const response: ConcordanceResult = {
    word,
    translation,
    limit,
    truncated,
    occurrences,
  };

  // Include total_count only when results are truncated.
  // The count was already fetched in the batch above — no extra round-trip needed.
  if (truncated && countResult.results.length > 0) {
    response.total_count = countResult.results[0]['total'] as number;
  }

  return response;
};

concordance.annotations = {
  readOnlyHint: true,
  destructiveHint: false,
  openWorldHint: true,
};

concordance.description =
  'Survey every occurrence of a word across the entire Bible, grouped by book in canonical order. ' +
  'Searches single words only; use find_text for phrases. ' +
  'Use this for complete word studies (e.g. how many times does "grace" appear in each book?). ' +
  'Unlike find_text, results are grouped by book with per-book counts; when truncated, total_count shows the full match count. ' +
  'Optionally filter by translation (KJV, WEB, ASV, YLT, Darby). For phrase searches or faster spot checks, use find_text instead.';

concordance.input = {
  word: T.string({
    required: true,
    description: 'Word to look up across all Bible verses.',
    minLength: 1,
  }),
  translation: T.string({
    required: false,
    description:
      'Translation abbreviation to restrict the search (e.g. "KJV", "WEB", "ASV"). ' +
      'Omit to search across all translations.',
  }),
  limit: T.number({
    required: false,
    description: `Maximum number of verses to return. Default ${CONCORDANCE_DEFAULT_LIMIT}, max ${CONCORDANCE_MAX_LIMIT}.`,
    min: 1,
    max: CONCORDANCE_MAX_LIMIT,
    default: CONCORDANCE_DEFAULT_LIMIT,
  }),
};

export default concordance;
