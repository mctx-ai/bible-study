// Resource: bible://{translation}/{book}/{chapter}/{verse}
//
// Returns a specific verse plus 3-5 surrounding verses for context. The
// requested verse is marked with `requested: true` so callers can identify
// it within the context window. All verses carry structured Citation objects.

import type { ResourceHandler } from '@mctx-ai/app';
import { d1 } from '../lib/cloudflare.js';
import {
  getTranslation,
  isValidTranslation,
  resolveBook,
  makeCitation,
  ensureInitialized,
} from '../lib/bible-utils.js';
import type { Citation } from '../lib/bible-utils.js';

// Number of verses to include before and after the requested verse.
const CONTEXT_BEFORE = 2;
const CONTEXT_AFTER = 3;

interface VerseRow {
  verse: number;
  text: string;
}

interface VerseResult {
  citation: Citation;
  text: string;
  requested?: true;
}

interface VerseWithContextResult {
  translation: string;
  book: string;
  chapter: number;
  requestedVerse: number;
  verses: VerseResult[];
}

interface ErrorResult {
  error: string;
}

const handler: ResourceHandler = async (params) => {
  await ensureInitialized();

  const {
    translation: translationParam,
    book,
    chapter,
    verse,
  } = params as {
    translation: string;
    book: string;
    chapter: string;
    verse: string;
  };

  const translationUpper = translationParam.toUpperCase();
  if (!isValidTranslation(translationUpper)) {
    const result: ErrorResult = {
      error: `Unknown translation: "${translationParam}". Use bible://translations to list available translations.`,
    };
    return JSON.stringify(result);
  }

  const resolvedBook = resolveBook(book);
  if (!resolvedBook) {
    const result: ErrorResult = {
      error: `Unknown book: "${book}". Check spelling or use a common abbreviation (e.g. Gen, Matt, Rev).`,
    };
    return JSON.stringify(result);
  }

  const chapterNum = parseInt(chapter, 10);
  if (!Number.isInteger(chapterNum) || chapterNum < 1) {
    const result: ErrorResult = {
      error: `Chapter must be a positive integer; got "${chapter}".`,
    };
    return JSON.stringify(result);
  }

  const verseNum = parseInt(verse, 10);
  if (!Number.isInteger(verseNum) || verseNum < 1) {
    const result: ErrorResult = {
      error: `Verse must be a positive integer; got "${verse}".`,
    };
    return JSON.stringify(result);
  }

  const translation = getTranslation(translationUpper);
  if (!translation) {
    // This path should not be reached because isValidTranslation guards above,
    // but guard defensively in case the cache is not yet populated.
    const result: ErrorResult = {
      error: `Translation "${translationUpper}" not found in cache. Try again after initialization.`,
    };
    return JSON.stringify(result);
  }

  const minVerse = Math.max(1, verseNum - CONTEXT_BEFORE);
  const maxVerse = verseNum + CONTEXT_AFTER;

  const queryResult = await d1.query(
    `SELECT v.verse, v.text
       FROM verses v
      WHERE v.translation_id = ?
        AND v.book_id = ?
        AND v.chapter = ?
        AND v.verse >= ?
        AND v.verse <= ?
      ORDER BY v.verse`,
    [translation.id, resolvedBook.id, chapterNum, minVerse, maxVerse]
  );

  if (queryResult.results.length === 0) {
    const result: ErrorResult = {
      error: `No verses found near ${resolvedBook.name} ${chapterNum}:${verseNum} in ${translationUpper}. Verify the reference exists.`,
    };
    return JSON.stringify(result);
  }

  // Check the requested verse is actually present in the result set.
  const rows = queryResult.results as unknown as VerseRow[];
  const requestedPresent = rows.some((row) => row.verse === verseNum);
  if (!requestedPresent) {
    const result: ErrorResult = {
      error: `Verse ${verseNum} does not exist in ${resolvedBook.name} chapter ${chapterNum} (${translationUpper}).`,
    };
    return JSON.stringify(result);
  }

  const verses: VerseResult[] = rows.map((row) => {
    const entry: VerseResult = {
      citation: makeCitation(resolvedBook, chapterNum, row.verse, translationUpper),
      text: row.text,
    };
    if (row.verse === verseNum) {
      entry.requested = true;
    }
    return entry;
  });

  const result: VerseWithContextResult = {
    translation: translationUpper,
    book: resolvedBook.name,
    chapter: chapterNum,
    requestedVerse: verseNum,
    verses,
  };

  return JSON.stringify(result);
};

handler.description =
  'Returns a specific Bible verse with surrounding context (2 verses before and 3 after) in a given translation. The requested verse is flagged in the response. Context clips at chapter boundaries.';
handler.mimeType = 'application/json';

export default handler;
