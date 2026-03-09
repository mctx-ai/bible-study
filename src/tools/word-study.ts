// word-study.ts — Original language analysis for a word in a verse.
//
// Accepts a verse reference (book, chapter, verse) and a word parameter that
// can be either an English surface form or a word position string (e.g. '1', '2a').
//
// Flow:
//   1. Resolve book via alias resolver.
//   2. Query morphology table for the verse.
//   3. Match word param against word_position (coerced to string) first;
//      fall back to matching the English surface form via the verse text.
//   4. From the matched morphology row, get strongs_number.
//   5. Query strongs table for the Strong's entry.
//   6. Query lexicon_entries for BDB/Thayer definition.
//   7. Query morphology for other verses with the same strongs_number (LIMIT 20).
//   8. Count total occurrences (distinct verses with that strongs_number).

import type { ToolHandler } from '@mctx-ai/mcp-server';
import { T } from '@mctx-ai/mcp-server';
import { d1 } from '../lib/cloudflare.js';
import {
  makeCitation,
  validateVerseRef,
  ensureInitialized,
} from '../lib/bible-utils.js';
import type { Citation } from '../lib/bible-utils.js';

// ─── Response shape ───────────────────────────────────────────────────────────

interface LexiconDef {
  short_def: string;
  long_def: string;
}

interface MorphologyInfo {
  lemma: string;
  parsing: string;
}

interface OtherOccurrence {
  text: string;
  citation: Citation;
}

interface WordStudyResult {
  original_word: string;
  strongs_number: string;
  transliteration: string;
  definition: string;
  lexicon: LexiconDef;
  morphology: MorphologyInfo;
  other_occurrences: OtherOccurrence[];
  total_occurrences: number;
  citation: Citation;
}

// ─── Handler ──────────────────────────────────────────────────────────────────

const wordStudy: ToolHandler = async (args, _ask?) => {
  await ensureInitialized();

  const { book, chapter, verse, word } = args as {
    book: string;
    chapter: number;
    verse: number;
    word: string;
  };

  // 1. Resolve book.
  const validation = validateVerseRef(book, chapter, verse);
  if ('error' in validation) {
    throw new Error(validation.error);
  }
  const resolvedBook = validation.book;

  // 2. Query all morphology rows for this verse (all translations).
  //    We use translation_id-agnostic matching since morphology is keyed by
  //    position, not translation. We pick the first matching row.
  //    We also JOIN to verses here so that matchByEnglishSurface has the verse
  //    text available without issuing an additional round-trip query.
  // KJV is translation_id 1. Morphology rows use translation_id 6 (Hebrew/TAHOT)
  // or 7 (Greek/TAGNT), so we cannot join verses on m.translation_id — that
  // would always produce NULL verse_text. Instead, join to KJV (id=1) using
  // only the book/chapter/verse coordinates.
  const KJV_TRANSLATION_ID = 1;

  const morphResult = await d1.query(
    `SELECT
       m.id,
       m.word_position,
       m.strongs_number,
       m.lemma,
       m.parsing,
       m.translation_id,
       v.text AS verse_text
     FROM morphology m
     LEFT JOIN verses v
       ON v.book_id = m.book_id
       AND v.chapter = m.chapter
       AND v.verse = m.verse
       AND v.translation_id = ?
     WHERE m.book_id = ? AND m.chapter = ? AND m.verse = ?
     ORDER BY m.word_position`,
    [KJV_TRANSLATION_ID, resolvedBook.id, chapter, verse]
  );

  if (morphResult.results.length === 0) {
    throw new Error(
      `No morphology data found for ${resolvedBook.name} ${chapter}:${verse}. ` +
        'Original language data may not be available for this verse.'
    );
  }

  // 3a. Try matching word_position first (coerce param to string for comparison).
  const wordParam = String(word).trim();
  let matchedRow = morphResult.results.find(
    (row) => String(row['word_position']) === wordParam
  );

  // 3b. Fall back: match English surface form against verse text.
  //    The verse text was fetched via JOIN in step 2, so we extract it from
  //    the first morphology row — no additional query needed.
  if (!matchedRow) {
    const verseText = morphResult.results[0]?.['verse_text'] as string | undefined;
    matchedRow = matchByEnglishSurface(wordParam, verseText, morphResult.results);
  }

  if (!matchedRow) {
    throw new Error(
      `Word "${word}" not found in ${resolvedBook.name} ${chapter}:${verse}. ` +
        `Try a word position (e.g. "1", "2", "3") or an English word that appears in the verse.`
    );
  }

  // 4. Extract strongs_number from matched morphology row.
  const strongsNumber = matchedRow['strongs_number'] as string | null;
  if (!strongsNumber) {
    throw new Error(
      `No Strong's number found for word at position ${matchedRow['word_position']} ` +
        `in ${resolvedBook.name} ${chapter}:${verse}.`
    );
  }

  // 5–8. Batch all remaining queries.
  const [strongsResult, lexiconResult, otherVersesMorphResult, countResult] =
    await d1.batch([
      // 5. Strong's entry.
      {
        sql: `SELECT original_word, transliteration, definition, language
              FROM strongs
              WHERE prefixed_number = ?`,
        params: [strongsNumber],
      },
      // 6. Lexicon entry (BDB for Hebrew, Thayer for Greek).
      {
        sql: `SELECT short_def, long_def
              FROM lexicon_entries
              WHERE strongs_number = ?
              LIMIT 1`,
        params: [strongsNumber],
      },
      // 7. Other verses with the same strongs_number (up to 20, canonical order).
      {
        sql: `SELECT DISTINCT m.book_id, m.chapter, m.verse, m.translation_id
              FROM morphology m
              WHERE m.strongs_number = ?
                AND NOT (m.book_id = ? AND m.chapter = ? AND m.verse = ?)
              ORDER BY m.book_id, m.chapter, m.verse
              LIMIT 20`,
        params: [strongsNumber, resolvedBook.id, chapter, verse],
      },
      // 8. Total occurrence count (distinct verses).
      //    String concatenation with '.' as separator is safe here because
      //    book_id, chapter, and verse are all integers, so a '.' never
      //    appears in any component value — making each composite key
      //    unambiguous (e.g. "1.2.3" can only mean book 1, chapter 2, verse 3).
      {
        sql: `SELECT COUNT(DISTINCT (book_id || '.' || chapter || '.' || verse)) AS total
              FROM morphology
              WHERE strongs_number = ?`,
        params: [strongsNumber],
      },
    ]);

  // 5. Parse Strong's entry.
  const strongsRow = strongsResult.results[0];
  if (!strongsRow) {
    throw new Error(
      `Strong's entry not found for number ${strongsNumber}. The database may be incomplete.`
    );
  }

  // 6. Parse lexicon entry (may be absent for some numbers).
  const lexiconRow = lexiconResult.results[0];

  // 7 & 8. Parse other occurrences and count.
  const totalOccurrences = (countResult.results[0]?.['total'] as number) ?? 0;

  // Fetch verse texts for other occurrences to build human-readable results.
  const otherOccurrences = await buildOtherOccurrences(
    otherVersesMorphResult.results
  );

  // Build source citation — use the first translation_id found for this verse.
  const sourceCitation = makeCitation(
    resolvedBook,
    chapter,
    verse,
    'ORIG' // Morphology is translation-independent; use canonical marker.
  );

  const result: WordStudyResult = {
    original_word: (strongsRow['original_word'] as string) ?? '',
    strongs_number: strongsNumber,
    transliteration: (strongsRow['transliteration'] as string) ?? '',
    definition: (strongsRow['definition'] as string) ?? '',
    lexicon: {
      short_def: (lexiconRow?.['short_def'] as string) ?? '',
      long_def: (lexiconRow?.['long_def'] as string) ?? '',
    },
    morphology: {
      lemma: (matchedRow['lemma'] as string) ?? '',
      parsing: (matchedRow['parsing'] as string) ?? '',
    },
    other_occurrences: otherOccurrences,
    total_occurrences: totalOccurrences,
    citation: sourceCitation,
  };

  return result;
};

// ─── Helpers ──────────────────────────────────────────────────────────────────

/**
 * Match an English surface form against the verse text.
 *
 * Splits verseText into words and finds the position of the matching word,
 * then looks up that position in the morphology rows.
 *
 * verseText is provided by the caller — it was retrieved via JOIN in the
 * initial morphology query, so no additional DB round-trip is needed here.
 *
 * Word positions in morphology are 1-indexed. Positions like '1a'/'1b' are
 * kept as-is (they represent subdivided tokens in the original languages).
 */
function matchByEnglishSurface(
  wordParam: string,
  verseText: string | undefined,
  morphRows: Record<string, unknown>[]
): Record<string, unknown> | undefined {
  if (!verseText) return undefined;

  // Split on whitespace and strip punctuation for comparison.
  const verseWords = verseText
    .split(/\s+/)
    .map((w) => w.replace(/[^\w'-]/g, '').toLowerCase());

  const targetWord = wordParam.toLowerCase();
  const wordIndex = verseWords.findIndex((w) => w === targetWord);

  if (wordIndex === -1) return undefined;

  // word_position is 1-indexed; match integer positions first.
  const position = String(wordIndex + 1);
  return morphRows.find((row) => String(row['word_position']) === position);
}

/**
 * Given a list of morphology rows (with book_id, chapter, verse, translation_id),
 * fetch verse texts and build OtherOccurrence objects.
 *
 * We bulk-fetch verse texts using a single query with OR conditions to avoid
 * N+1 queries (up to 20 rows).
 */
async function buildOtherOccurrences(
  rows: Record<string, unknown>[]
): Promise<OtherOccurrence[]> {
  if (rows.length === 0) return [];

  // Morphology rows use translation_id 6 (Hebrew) or 7 (Greek). English verse
  // text only exists for translation_id 1-5. Always query KJV (id=1) for
  // other-occurrence verse text.
  const KJV_TRANSLATION_ID = 1;

  // Build a single query with UNION ALL to fetch all needed verses at once.
  // Each row has book_id, chapter, verse. We deduplicate by verse reference.
  const seen = new Set<string>();
  const uniqueRefs: { bookId: number; chapter: number; verse: number }[] = [];

  for (const row of rows) {
    const key = `${row['book_id']}.${row['chapter']}.${row['verse']}`;
    if (!seen.has(key)) {
      seen.add(key);
      uniqueRefs.push({
        bookId: row['book_id'] as number,
        chapter: row['chapter'] as number,
        verse: row['verse'] as number,
      });
    }
  }

  if (uniqueRefs.length === 0) return [];

  // Build WHERE clause for bulk verse fetch. All rows use the same KJV
  // translation_id, so we only parameterise book/chapter/verse.
  const conditions = uniqueRefs
    .map(() => '(v.book_id = ? AND v.chapter = ? AND v.verse = ?)')
    .join(' OR ');

  const params: unknown[] = uniqueRefs.flatMap((r) => [
    r.bookId,
    r.chapter,
    r.verse,
  ]);

  const versesResult = await d1.query(
    `SELECT v.book_id, v.chapter, v.verse, v.text, v.translation_id,
            b.name AS book_name, t.abbreviation AS translation_abbrev
     FROM verses v
     JOIN books b ON b.id = v.book_id
     JOIN translations t ON t.id = v.translation_id
     WHERE v.translation_id = ? AND (${conditions})`,
    [KJV_TRANSLATION_ID, ...params]
  );

  // Build a lookup map for fast access.
  const verseMap = new Map<string, Record<string, unknown>>();
  for (const row of versesResult.results) {
    const key = `${row['book_id']}.${row['chapter']}.${row['verse']}`;
    if (!verseMap.has(key)) {
      verseMap.set(key, row);
    }
  }

  const occurrences: OtherOccurrence[] = [];

  for (const ref of uniqueRefs) {
    const key = `${ref.bookId}.${ref.chapter}.${ref.verse}`;
    const row = verseMap.get(key);
    if (!row) continue;

    const citation: Citation = {
      book: row['book_name'] as string,
      chapter: ref.chapter,
      verse: ref.verse,
      translation: row['translation_abbrev'] as string,
    };

    occurrences.push({
      text: row['text'] as string,
      citation,
    });
  }

  return occurrences;
}

// ─── Metadata ─────────────────────────────────────────────────────────────────

wordStudy.description =
  'Perform an original language word study for a specific word in a Bible verse. ' +
  'You MUST provide an exact verse reference (book, chapter, verse). Use search_bible or find_text first if you need to locate the verse. ' +
  'The word parameter accepts position strings (e.g. "1", "2a") or an English surface form. ' +
  'Returns the Hebrew or Greek word, Strong\'s number, transliteration, definition, ' +
  'lexicon entry (BDB for Hebrew, Thayer for Greek), morphological parsing, and a ' +
  'list of other verses where the same word appears.';

wordStudy.input = {
  book: T.string({
    required: true,
    description:
      'Book name or alias (e.g. "Genesis", "Gen", "John", "1 Cor"). ' +
      'Common abbreviations and alternate spellings are accepted.',
  }),
  chapter: T.number({
    required: true,
    description: 'Chapter number (positive integer).',
    min: 1,
  }),
  verse: T.number({
    required: true,
    description: 'Verse number (positive integer).',
    min: 1,
  }),
  word: T.string({
    required: true,
    description:
      'The word to study. Accepts either: ' +
      '(1) a word position string (e.g. "1", "2", "3", "1a", "1b") corresponding ' +
      'to the word\'s position in the original language text, or ' +
      '(2) an English surface form (e.g. "love", "grace") that will be matched ' +
      'against the verse text to determine its position.',
    minLength: 1,
  }),
};

export default wordStudy;
