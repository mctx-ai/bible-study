// Resource: bible://{translation}/{book}/{chapter}/{verse}
//
// Returns a specific verse plus 3-5 surrounding verses for context. The
// requested verse is marked with `requested: true` so callers can identify
// it within the context window. All verses carry structured Citation objects.

import type { ResourceHandler } from '@mctx-ai/mcp-server';
import { d1 } from '../lib/cloudflare.js';
import {
  isValidTranslation,
  resolveBook,
  makeCitation,
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
  const { translation, book, chapter, verse } = params as {
    translation: string;
    book: string;
    chapter: string;
    verse: string;
  };

  const translationUpper = translation.toUpperCase();
  if (!isValidTranslation(translationUpper)) {
    const result: ErrorResult = {
      error: `Unknown translation: "${translation}". Use bible://translations to list available translations.`,
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

  const minVerse = Math.max(1, verseNum - CONTEXT_BEFORE);
  const maxVerse = verseNum + CONTEXT_AFTER;

  const queryResult = await d1.query(
    `SELECT v.verse, v.text
       FROM verses v
       JOIN translations t ON t.id = v.translation_id
       JOIN books b ON b.id = v.book_id
      WHERE t.abbreviation = ?
        AND b.id = ?
        AND v.chapter = ?
        AND v.verse >= ?
        AND v.verse <= ?
      ORDER BY v.verse`,
    [translationUpper, resolvedBook.id, chapterNum, minVerse, maxVerse]
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
  'Returns a specific Bible verse in a given translation plus 2 verses before and 3 after for reading context. Context clips at chapter boundaries — verses from adjacent chapters are not included. The requested verse is marked requested: true in the response. Use this for direct verse lookup when you know the exact reference. Translations: KJV, WEB, ASV, YLT, Darby.';
handler.mimeType = 'application/json';

export default handler;
