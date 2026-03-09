// compare-translations tool
//
// Returns the same passage from all 5 translations side-by-side.
// Each verse is fully cited with a structured Citation object.

import type { ToolHandler } from '@mctx-ai/mcp-server';
import { T } from '@mctx-ai/mcp-server';
import { d1 } from '../lib/cloudflare.js';
import {
  resolveBook,
  getAllTranslations,
  makeCitation,
  ensureInitialized,
} from '../lib/bible-utils.js';
import type { Citation } from '../lib/bible-utils.js';

// ─── Response shape ───────────────────────────────────────────────────────────

interface TranslationEntry {
  text: string;
  citation: Citation;
}

interface VerseComparison {
  verse: number;
  translations: TranslationEntry[];
}

interface CompareTranslationsResult {
  book: string;
  chapter: number;
  verse_start: number;
  verse_end: number;
  verses: VerseComparison[];
}

// ─── Handler ──────────────────────────────────────────────────────────────────

const compareTranslations: ToolHandler = async (args, _ask?) => {
  await ensureInitialized();

  const { book: bookInput, chapter, verse_start, verse_end } = args as {
    book: string;
    chapter: number;
    verse_start: number;
    verse_end: number | undefined;
  };

  const resolvedVerseEnd = verse_end ?? verse_start;

  // Validate book
  const book = resolveBook(bookInput);
  if (!book) {
    throw new Error(`Unknown book: "${bookInput}". Please provide a valid book name or alias.`);
  }

  // Validate chapter and verse range
  if (!Number.isInteger(chapter) || chapter < 1) {
    throw new Error(`Chapter must be a positive integer; got ${chapter}`);
  }
  if (!Number.isInteger(verse_start) || verse_start < 1) {
    throw new Error(`verse_start must be a positive integer; got ${verse_start}`);
  }
  if (!Number.isInteger(resolvedVerseEnd) || resolvedVerseEnd < verse_start) {
    throw new Error(
      `verse_end must be >= verse_start (${verse_start}); got ${resolvedVerseEnd}`
    );
  }

  const translations = getAllTranslations();
  if (translations.length === 0) {
    throw new Error('No translations available. Database may not be initialized.');
  }

  // Fetch all verses for the range and all translations in a single query.
  // Join with translations table to get abbreviation alongside the text.
  const result = await d1.query(
    `SELECT v.verse, v.text, t.abbreviation AS translation_abbrev
     FROM verses v
     JOIN translations t ON t.id = v.translation_id
     WHERE v.book_id = ?
       AND v.chapter = ?
       AND v.verse >= ?
       AND v.verse <= ?
     ORDER BY v.verse ASC, t.abbreviation ASC`,
    [book.id, chapter, verse_start, resolvedVerseEnd]
  );

  // Group rows by verse number
  const verseMap = new Map<number, TranslationEntry[]>();

  for (const row of result.results) {
    const verseNum = row['verse'] as number;
    const text = row['text'] as string;
    const translationAbbrev = row['translation_abbrev'] as string;

    if (!verseMap.has(verseNum)) {
      verseMap.set(verseNum, []);
    }

    verseMap.get(verseNum)!.push({
      text,
      citation: makeCitation(book, chapter, verseNum, translationAbbrev),
    });
  }

  // Build ordered verses array covering the full requested range
  const verses: VerseComparison[] = [];
  for (let v = verse_start; v <= resolvedVerseEnd; v++) {
    const translationEntries = verseMap.get(v) ?? [];
    verses.push({ verse: v, translations: translationEntries });
  }

  const response: CompareTranslationsResult = {
    book: book.name,
    chapter,
    verse_start,
    verse_end: resolvedVerseEnd,
    verses,
  };

  return response;
};

compareTranslations.annotations = {
  readOnlyHint: true,
  destructiveHint: false,
  openWorldHint: true,
};

compareTranslations.description =
  'Compare the same Bible passage side-by-side across all 5 translations (KJV, WEB, ASV, YLT, Darby). ' +
  'Accepts a book name or alias, chapter, and verse range. Returns every verse with its text and a ' +
  'structured citation for each translation.';

compareTranslations.input = {
  book: T.string({
    required: true,
    description: 'Book name or alias (e.g. "Genesis", "Gen", "1 Cor", "Revelation")',
    minLength: 1,
  }),
  chapter: T.number({
    required: true,
    description: 'Chapter number (1-based)',
    min: 1,
  }),
  verse_start: T.number({
    required: true,
    description: 'First verse of the range (1-based)',
    min: 1,
  }),
  verse_end: T.number({
    required: false,
    description:
      'Last verse of the range (inclusive). Defaults to verse_start for a single-verse lookup.',
    min: 1,
  }),
};

export default compareTranslations;
