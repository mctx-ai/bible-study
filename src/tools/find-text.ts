// find-text.ts — keyword search tool using FTS5 full-text search
//
// Searches Bible verses using SQLite FTS5 MATCH queries. User input is
// sanitized by wrapping in double quotes to prevent FTS5 metacharacter
// injection (AND, OR, NOT, NEAR, *, quotes, etc.).

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

// ─── Shared FTS5 sanitization ─────────────────────────────────────────────────

/**
 * Sanitizes a user-supplied string for safe use in an FTS5 MATCH expression.
 *
 * Multi-word inputs are wrapped in double quotes to produce an exact phrase
 * search, neutralizing all FTS5 metacharacters (AND, OR, NOT, NEAR, *, ^,
 * parentheses, etc.). Any embedded double-quote characters are escaped by
 * doubling them ("" is the SQLite FTS5 escape for a literal quote inside a
 * phrase).
 *
 * Single-word inputs are NOT quoted so that FTS5 stemming remains active.
 * Quoting a single word disables stemming, meaning "loves" would not match
 * verses containing only "love". Special characters within single words are
 * still escaped via quoting.
 *
 * @example
 *   sanitizeFts5('God so loved')  → '"God so loved"'
 *   sanitizeFts5('loves')         → 'loves'
 *   sanitizeFts5('it\'s "good"')  → '"it\'s ""good"""'
 */
export function sanitizeFts5(input: string): string {
  const trimmed = input.trim();
  const escaped = trimmed.replace(/"/g, '""');
  // Only quote multi-word phrases; single words pass through to preserve stemming.
  // Single words may still contain special chars that need quoting (e.g. apostrophes
  // in some FTS5 tokenizer configs), so we quote if any FTS5 metacharacter is present.
  const hasFts5Meta = /[\s"*^()\-:]/.test(trimmed) || /\b(AND|OR|NOT|NEAR)\b/i.test(trimmed);
  if (hasFts5Meta) {
    return `"${escaped}"`;
  }
  return escaped;
}

// ─── find_text tool ───────────────────────────────────────────────────────────

interface FindTextResult {
  query: string;
  translation?: string;
  limit: number;
  count: number;
  verses: Array<{
    citation: Citation;
    text: string;
  }>;
}

const FIND_TEXT_DEFAULT_LIMIT = 20;
const FIND_TEXT_MAX_LIMIT = 100;

const findText: ToolHandler = async (args, _ask?) => {
  await ensureInitialized();

  const { query, translation, limit: rawLimit } = args as {
    query: string;
    translation?: string;
    limit?: number;
  };

  const limit = Math.max(1, Math.floor(Math.min(rawLimit ?? FIND_TEXT_DEFAULT_LIMIT, FIND_TEXT_MAX_LIMIT)));

  // Validate translation filter if provided.
  if (translation !== undefined && !isValidTranslation(translation)) {
    throw new Error(
      `Unknown translation "${translation}". Use the bible://translations resource to list available translations.`
    );
  }

  const ftsPhrase = sanitizeFts5(query);

  let sql: string;
  let params: unknown[];

  if (translation !== undefined) {
    const t = getTranslation(translation);
    sql = `
      SELECT v.id, v.chapter, v.verse, v.text, b.name AS book_name, t.abbreviation AS translation_abbrev
      FROM verses_fts f
      JOIN verses v ON f.rowid = v.id
      JOIN books b ON v.book_id = b.id
      JOIN translations t ON v.translation_id = t.id
      WHERE verses_fts MATCH ?
        AND v.translation_id = ?
      ORDER BY b.canonical_order, v.chapter, v.verse
      LIMIT ?
    `.trim();
    params = [ftsPhrase, t!.id, limit];
  } else {
    sql = `
      SELECT v.id, v.chapter, v.verse, v.text, b.name AS book_name, t.abbreviation AS translation_abbrev
      FROM verses_fts f
      JOIN verses v ON f.rowid = v.id
      JOIN books b ON v.book_id = b.id
      JOIN translations t ON v.translation_id = t.id
      WHERE verses_fts MATCH ?
      ORDER BY b.canonical_order, v.chapter, v.verse
      LIMIT ?
    `.trim();
    params = [ftsPhrase, limit];
  }

  const result = await d1.query(sql, params);

  const verses = result.results.map((row) => {
    const bookName = row['book_name'] as string;
    const chapter = row['chapter'] as number;
    const verse = row['verse'] as number;
    const translationAbbrev = row['translation_abbrev'] as string;
    const text = row['text'] as string;

    // resolveBook is cached in-memory — no additional DB query.
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

    return { citation, text };
  });

  const response: FindTextResult = {
    query,
    translation,
    limit,
    count: verses.length,
    verses,
  };

  return response;
};

findText.annotations = {
  readOnlyHint: true,
  destructiveHint: false,
  openWorldHint: true,
};

findText.description =
  'Find Bible verses containing an exact, case-insensitive word or phrase using full-text search. ' +
  'Use this when you know the specific wording to search for (e.g. "consider the lilies", "fear not"). ' +
  'Faster than search_bible and returns matches in canonical order (Genesis to Revelation). ' +
  'Use concordance instead when you need all occurrences of a single word grouped by book with totals. ' +
  'Optionally filter by translation (KJV, WEB, ASV, YLT, Darby).';

findText.input = {
  query: T.string({
    required: true,
    description: 'Keyword or phrase to search for in verse text.',
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
    description: `Maximum number of verses to return. Default ${FIND_TEXT_DEFAULT_LIMIT}, max ${FIND_TEXT_MAX_LIMIT}.`,
    min: 1,
    max: FIND_TEXT_MAX_LIMIT,
    default: FIND_TEXT_DEFAULT_LIMIT,
  }),
};

export default findText;
