// Resource: bible://{translation}/{book}/{chapter}
//
// Returns the full text of a Bible chapter with structured Citation objects
// per verse. Book names are resolved via the alias resolver so common
// variants (Gen, gen, Genesis) all work.

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

interface VerseRow {
  verse: number;
  text: string;
}

interface VerseResult {
  citation: Citation;
  text: string;
}

interface ChapterResult {
  translation: string;
  book: string;
  chapter: number;
  verses: VerseResult[];
}

interface ErrorResult {
  error: string;
}

const handler: ResourceHandler = async (params) => {
  await ensureInitialized();

  const { translation: translationParam, book, chapter } = params as {
    translation: string;
    book: string;
    chapter: string;
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

  const translation = getTranslation(translationUpper);
  if (!translation) {
    // This path should not be reached because isValidTranslation guards above,
    // but guard defensively in case the cache is not yet populated.
    const result: ErrorResult = {
      error: `Translation "${translationUpper}" not found in cache. Try again after initialization.`,
    };
    return JSON.stringify(result);
  }

  const queryResult = await d1.query(
    `SELECT v.verse, v.text
       FROM verses v
      WHERE v.translation_id = ?
        AND v.book_id = ?
        AND v.chapter = ?
      ORDER BY v.verse`,
    [translation.id, resolvedBook.id, chapterNum]
  );

  if (queryResult.results.length === 0) {
    const result: ErrorResult = {
      error: `No verses found for ${resolvedBook.name} chapter ${chapterNum} in ${translationUpper}. The chapter may not exist in this translation.`,
    };
    return JSON.stringify(result);
  }

  const verses: VerseResult[] = (queryResult.results as unknown as VerseRow[]).map((row) => ({
    citation: makeCitation(resolvedBook, chapterNum, row.verse, translationUpper),
    text: row.text,
  }));

  const result: ChapterResult = {
    translation: translationUpper,
    book: resolvedBook.name,
    chapter: chapterNum,
    verses,
  };

  return JSON.stringify(result);
};

handler.description =
  'Returns every verse in a Bible chapter with citations, in a specific translation. Book names accept full names and common abbreviations (Gen, Matt, 1 Cor, Rev).';
handler.mimeType = 'application/json';

export default handler;
