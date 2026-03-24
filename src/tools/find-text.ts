// find-text.ts — keyword search tool using FTS5 full-text search
//
// Searches Bible verses using SQLite FTS5 MATCH queries. Multi-word input is
// split into individual keywords (stop words removed) joined by implicit AND.
// User-supplied double-quoted phrases are preserved as exact phrase matches.
// FTS5 metacharacters are stripped from all unquoted words.

import type { ToolHandler } from '@mctx-ai/app';
import { T } from '@mctx-ai/app';
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

/** Common English stop words stripped from multi-word queries. */
const STOP_WORDS = new Set([
  'a', 'an', 'the', 'in', 'of', 'for', 'to', 'is', 'it', 'and', 'or',
  'but', 'with', 'by', 'at', 'on', 'from', 'as', 'be', 'was', 'were',
  'been', 'are', 'am', 'do', 'does', 'did', 'has', 'have', 'had', 'this',
  'that', 'these', 'those', 'so', 'if', 'not', 'no', 'up', 'out', 'its',
]);

/** Strip FTS5 metacharacters from an individual word. */
function stripFts5Meta(word: string): string {
  return word.replace(/["*^()\-:]/g, '');
}

/**
 * Sanitizes a user-supplied string for safe use in an FTS5 MATCH expression.
 *
 * - If the input contains explicit double-quoted phrases, those phrases are
 *   preserved as FTS5 exact phrase matches (with internal quotes escaped).
 *   Any unquoted words surrounding the phrase are sanitized individually and
 *   joined with spaces (implicit AND).
 *
 * - Multi-word inputs without quotes are split into individual words, common
 *   English stop words are removed, FTS5 metacharacters are stripped, and the
 *   remaining words are joined with spaces (FTS5 implicit AND). This means
 *   'hope in suffering' becomes 'hope suffering' and matches verses containing
 *   both words in any order.
 *
 * - Single-word inputs pass through with metacharacters stripped, preserving
 *   FTS5 stemming (e.g. 'loves' still matches 'love').
 *
 * @example
 *   sanitizeFts5('hope in suffering')       → 'hope suffering'
 *   sanitizeFts5('loves')                   → 'loves'
 *   sanitizeFts5('"God so loved"')          → '"God so loved"'
 *   sanitizeFts5('Jesus "son of man"')      → 'Jesus "son of man"'
 *   sanitizeFts5('it\'s good')              → 'its good'
 */
export function sanitizeFts5(input: string): string {
  const trimmed = input.trim();
  if (!trimmed) return '';

  // Check for user-supplied explicit double quotes indicating exact phrase intent.
  // We detect quoted segments and preserve them, while sanitizing the rest as keywords.
  const hasExplicitQuotes = /".+"/.test(trimmed);

  if (hasExplicitQuotes) {
    // Extract quoted phrases and unquoted segments.
    const parts: string[] = [];
    const regex = /"([^"]+)"|(\S+)/g;
    let match: RegExpExecArray | null;
    while ((match = regex.exec(trimmed)) !== null) {
      if (match[1] !== undefined) {
        // Quoted phrase — preserve as FTS5 phrase match, escape internal quotes.
        const escaped = match[1].replace(/"/g, '""');
        parts.push(`"${escaped}"`);
      } else if (match[2] !== undefined) {
        // Unquoted word — sanitize and keep if not a stop word.
        const word = stripFts5Meta(match[2]);
        if (word && !STOP_WORDS.has(word.toLowerCase())) {
          parts.push(word);
        }
      }
    }
    return parts.join(' ');
  }

  // No explicit quotes — split into words, remove stop words, strip metacharacters.
  const words = trimmed
    .split(/\s+/)
    .map(stripFts5Meta)
    .filter((w) => w !== '' && !STOP_WORDS.has(w.toLowerCase()));

  return words.join(' ');
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

  if (!ftsPhrase) {
    const response: FindTextResult = {
      query,
      translation,
      limit,
      count: 0,
      verses: [],
    };
    return { ...response, message: 'No searchable terms found — try more specific keywords.' };
  }

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
  idempotentHint: true,
  openWorldHint: true,
};

findText.description =
  'Keyword search — finds Bible verses containing specific words or phrases in their actual text. Use when you know the exact wording: "consider the lilies", "fear not", "in the beginning". ' +
  'Multi-word queries match all words in any order; wrap in double quotes for exact phrase matching. ' +
  'This is a text-match tool, not a thematic retrieval tool. For thematic or narrative-level retrieval, use topical_search instead. ' +
  'Returns results in canonical order (Genesis to Revelation). Optionally filter by translation.';

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
