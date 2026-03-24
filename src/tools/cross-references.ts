// cross-references tool
//
// Given a source verse, returns related passages from the cross_references table.
// Each result includes the referenced verse text (KJV) and structured Citations
// for both the source verse and the referenced verse.

import type { ToolHandler } from '@mctx-ai/app';
import { T } from '@mctx-ai/app';
import { d1 } from '../lib/cloudflare.js';
import {
  resolveBook,
  getTranslation,
  makeCitation,
  validateVerseRef,
  ensureInitialized,
} from '../lib/bible-utils.js';
import type { Citation } from '../lib/bible-utils.js';

// ─── Response shape ───────────────────────────────────────────────────────────

interface CrossReferenceEntry {
  text: string;
  citation: Citation;
  confidence: number | null;
}

interface CrossReferencesResult {
  source: {
    text: string;
    citation: Citation;
  };
  cross_references: CrossReferenceEntry[];
  total_returned: number;
}

// ─── Handler ──────────────────────────────────────────────────────────────────

const DEFAULT_TRANSLATION = 'KJV';
const DEFAULT_LIMIT = 20;

const crossReferences: ToolHandler = async (args, _ask?) => {
  await ensureInitialized();

  const { book: bookInput, chapter, verse, limit: rawLimit } = args as {
    book: string;
    chapter: number;
    verse: number;
    limit: number | undefined;
  };

  const limit = Math.floor(rawLimit ?? DEFAULT_LIMIT);

  // Validate book and verse reference
  const validation = validateVerseRef(bookInput, chapter, verse);
  if ('error' in validation) {
    throw new Error(validation.error);
  }
  const { book } = validation;

  if (!Number.isInteger(limit) || limit < 1) {
    throw new Error(`limit must be a positive integer; got ${limit}`);
  }
  if (limit > 100) {
    throw new Error(`limit must be <= 100; got ${limit}`);
  }

  // Resolve default translation (KJV)
  const translation = getTranslation(DEFAULT_TRANSLATION);
  if (!translation) {
    throw new Error(`Translation "${DEFAULT_TRANSLATION}" not found in database.`);
  }

  // Fetch source verse text and cross-references concurrently.
  const [sourceResult, xrefResult] = await Promise.all([
    d1.query(
      `SELECT text FROM verses
            WHERE book_id = ? AND chapter = ? AND verse = ? AND translation_id = ?
            LIMIT 1`,
      [book.id, chapter, verse, translation.id]
    ),
    d1.query(
      `SELECT
               cr.to_book_id,
               cr.to_chapter,
               cr.to_verse,
               cr.confidence,
               b.name  AS to_book_name,
               v.text  AS to_text
             FROM cross_references cr
             JOIN books b ON b.id = cr.to_book_id
             LEFT JOIN verses v
               ON v.book_id = cr.to_book_id
               AND v.chapter = cr.to_chapter
               AND v.verse   = cr.to_verse
               AND v.translation_id = ?
             WHERE cr.from_book_id = ?
               AND cr.from_chapter  = ?
               AND cr.from_verse    = ?
             ORDER BY cr.confidence DESC NULLS LAST, cr.id ASC
             LIMIT ?`,
      [translation.id, book.id, chapter, verse, limit]
    ),
  ]);

  if (sourceResult.results.length === 0) {
    throw new Error(
      `Verse not found: ${book.name} ${chapter}:${verse} (${DEFAULT_TRANSLATION}). ` +
        'The verse may not exist in the database or the reference is out of range.'
    );
  }

  const sourceText = sourceResult.results[0]['text'] as string;
  const sourceCitation = makeCitation(book, chapter, verse, DEFAULT_TRANSLATION);

  const crossReferenceEntries: CrossReferenceEntry[] = xrefResult.results.map(
    (row) => {
      const toBookName = row['to_book_name'] as string;
      const toChapter = row['to_chapter'] as number;
      const toVerse = row['to_verse'] as number;
      const toText = (row['to_text'] as string | null) ?? '[verse text not available]';
      const confidence = row['confidence'] as number | null;

      // Resolve target book for makeCitation (needs Book object)
      const toBook = resolveBook(toBookName);
      const toCitation: Citation = toBook
        ? makeCitation(toBook, toChapter, toVerse, DEFAULT_TRANSLATION)
        : {
            book: toBookName,
            chapter: toChapter,
            verse: toVerse,
            translation: DEFAULT_TRANSLATION,
          };

      return {
        text: toText,
        citation: toCitation,
        confidence,
      };
    }
  );

  const response: CrossReferencesResult = {
    source: {
      text: sourceText,
      citation: sourceCitation,
    },
    cross_references: crossReferenceEntries,
    total_returned: crossReferenceEntries.length,
  };

  return response;
};

crossReferences.annotations = {
  readOnlyHint: true,
  destructiveHint: false,
  idempotentHint: true,
  openWorldHint: true,
};

crossReferences.description =
  'Trace related passages from a specific Bible verse using 606,140 curated cross-references (results in KJV). Best when you already have a strong anchor verse and want to expand into a broader canonical network of related texts. ' +
  'Anchor verses are typically identified first via topical_search, find_text, semantic_search, concordance, or word_study. ' +
  'Returns the source verse and each referenced passage with text, citation, and confidence score. ' +
  'Use after identifying an anchor verse, not as the primary tool for first-pass discovery of a broad topic.';

crossReferences.input = {
  book: T.string({
    required: true,
    description: 'Book name or alias (e.g. "John", "Jn", "Romans", "Rev")',
    minLength: 1,
  }),
  chapter: T.number({
    required: true,
    description: 'Chapter number (1-based)',
    min: 1,
  }),
  verse: T.number({
    required: true,
    description: 'Verse number (1-based)',
    min: 1,
  }),
  limit: T.number({
    required: false,
    description: 'Maximum number of cross-references to return (1–100). Defaults to 20.',
    min: 1,
    max: 100,
    default: DEFAULT_LIMIT,
  }),
};

export default crossReferences;
