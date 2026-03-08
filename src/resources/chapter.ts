// Resource: bible://{translation}/{book}/{chapter}
//
// Returns the full text of a Bible chapter with structured Citation objects
// per verse. Book names are resolved via the alias resolver so common
// variants (Gen, gen, Genesis) all work.

import type { ResourceHandler } from '@mctx-ai/mcp-server';
import { d1 } from '../lib/cloudflare.js';
import {
  isValidTranslation,
  resolveBook,
  makeCitation,
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
  const { translation, book, chapter } = params as {
    translation: string;
    book: string;
    chapter: string;
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

  const queryResult = await d1.query(
    `SELECT v.verse, v.text
       FROM verses v
       JOIN translations t ON t.id = v.translation_id
       JOIN books b ON b.id = v.book_id
      WHERE t.abbreviation = ?
        AND b.id = ?
        AND v.chapter = ?
      ORDER BY v.verse`,
    [translationUpper, resolvedBook.id, chapterNum]
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
  'Returns every verse in a Bible chapter with a structured Citation per verse. Use this to read an entire chapter in a specific translation (KJV, WEB, ASV, YLT, Darby). Book names accept full names and common abbreviations (Gen, Matt, 1 Cor, Rev). Response may be large for long chapters (e.g., Psalm 119 has 176 verses).';
handler.mimeType = 'application/json';

export default handler;
