// word-study.ts — Original language analysis for a word in a verse.
//
// Accepts a verse reference (book, chapter, verse) and a word parameter that
// can be either an English surface form or a word position string (e.g. '1', '2a').
//
// Flow:
//   1. Resolve book via alias resolver.
//   2. Query morphology table for the verse, joined with strongs definitions.
//   3. Match word param:
//      a. If numeric/compound-position (e.g. '1', '1a'): match morphology rows
//         where word_position starts with the given integer prefix.
//      b. Otherwise: match via Strong's definition gloss (case-insensitive
//         word boundary match). No positional alignment with English verse text.
//   4. From the matched morphology row, get strongs_number.
//   5. Query strongs table for the Strong's entry.
//   6. Query lexicon_entries for BDB/Thayer definition.
//   7. Query morphology for other verses with the same strongs_number (LIMIT 20).
//   8. Count total occurrences (distinct verses with that strongs_number).

import type { ToolHandler } from '@mctx-ai/mcp-server';
import { T } from '@mctx-ai/mcp-server';
import { d1 } from '../lib/cloudflare.js';
import {
  getTranslation,
  isValidTranslation,
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
  matched_count: number;
  other_occurrences: OtherOccurrence[];
  total_occurrences: number;
  citation: Citation;
}

// ─── Handler ──────────────────────────────────────────────────────────────────

const wordStudy: ToolHandler = async (args, _ask?) => {
  await ensureInitialized();

  const { book, chapter, verse, word, translation } = args as {
    book: string;
    chapter: number;
    verse: number;
    word: string;
    translation?: string;
  };

  // Resolve translation ID for verse text. Falls back to KJV (id=1) when the
  // user doesn't specify a translation or specifies an unknown one.
  const KJV_TRANSLATION_ID = 1;
  let verseTranslationId = KJV_TRANSLATION_ID;
  let verseTranslationAbbrev = 'KJV';
  if (translation !== undefined && isValidTranslation(translation)) {
    const resolvedTranslation = getTranslation(translation);
    if (resolvedTranslation) {
      verseTranslationId = resolvedTranslation.id;
      verseTranslationAbbrev = resolvedTranslation.abbreviation;
    }
  }

  // 1. Resolve book.
  const validation = validateVerseRef(book, chapter, verse);
  if ('error' in validation) {
    throw new Error(validation.error);
  }
  const resolvedBook = validation.book;

  // 2. Query all morphology rows for this verse, joined with strongs definitions.
  //    The strongs JOIN provides English gloss/definition fields so that
  //    matchByEnglishGloss can match without positional alignment.
  //    Morphology rows use translation_id 6 (Hebrew/TAHOT) or 7 (Greek/TAGNT).
  const morphResult = await d1.query(
    `SELECT
       m.id,
       m.word_position,
       m.strongs_number,
       m.lemma,
       m.parsing,
       m.translation_id,
       s.definition AS strongs_definition
     FROM morphology m
     LEFT JOIN strongs s ON s.prefixed_number = m.strongs_number
     WHERE m.book_id = ? AND m.chapter = ? AND m.verse = ?
     ORDER BY m.word_position`,
    [resolvedBook.id, chapter, verse]
  );

  if (morphResult.results.length === 0) {
    throw new Error(
      `No morphology data found for ${resolvedBook.name} ${chapter}:${verse}. ` +
        'Original language data may not be available for this verse.'
    );
  }

  const wordParam = String(word).trim();

  // 3a. Try matching by original-language word position.
  //    Positional input: pure digits (e.g. '1', '2') or compound positions
  //    (e.g. '1a', '2b'). An integer input of '1' should match both '1' and
  //    compound sub-parts '1a', '1b' (all parts of the same source word).
  let matchedRow: Record<string, unknown> | undefined;
  let matchedCount = 0;

  if (/^\d+[a-z]*$/i.test(wordParam)) {
    matchedRow = matchByPosition(wordParam, morphResult.results);
    if (matchedRow) {
      matchedCount = 1; // Positional match is always unambiguous.
    }
  }

  // 3b. Fall back: match English word against Strong's definition glosses.
  //    This avoids positional alignment between English and Hebrew/Greek text
  //    entirely — matching happens through meaning instead.
  if (!matchedRow) {
    const { first, count } = matchByEnglishGloss(wordParam, morphResult.results);
    matchedRow = first;
    matchedCount = count;
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
      //    JOIN verses and books inline to return verse text and book name
      //    in a single round-trip, eliminating the separate fetchVerseTexts call.
      {
        sql: `SELECT DISTINCT m.book_id, m.chapter, m.verse,
                     v.text AS verse_text,
                     b.name AS book_name
              FROM morphology m
              JOIN verses v ON v.book_id = m.book_id
                           AND v.chapter  = m.chapter
                           AND v.verse    = m.verse
                           AND v.translation_id = ?
              JOIN books b ON b.id = m.book_id
              WHERE m.strongs_number = ?
                AND NOT (m.book_id = ? AND m.chapter = ? AND m.verse = ?)
              ORDER BY m.book_id, m.chapter, m.verse
              LIMIT 20`,
        params: [verseTranslationId, strongsNumber, resolvedBook.id, chapter, verse],
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

  // Build other occurrences directly from the inline JOIN result — no extra round-trip needed.
  const otherOccurrences = buildOtherOccurrencesInline(
    otherVersesMorphResult.results,
    verseTranslationAbbrev
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
    matched_count: matchedCount,
    other_occurrences: otherOccurrences,
    total_occurrences: totalOccurrences,
    citation: sourceCitation,
  };

  return result;
};

// ─── Helpers ──────────────────────────────────────────────────────────────────

/**
 * Match by original-language word position.
 *
 * If the param is a plain integer (e.g. '2'), it matches both '2' and any
 * compound sub-parts like '2a', '2b' — returning the first (primary) match.
 * If the param is already a compound position (e.g. '2a'), it matches exactly.
 */
function matchByPosition(
  wordParam: string,
  morphRows: Record<string, unknown>[]
): Record<string, unknown> | undefined {
  const lower = wordParam.toLowerCase();

  // Exact match first (handles '1a', '2b', or plain '1' when only one row).
  const exact = morphRows.find(
    (row) => String(row['word_position']).toLowerCase() === lower
  );
  if (exact) return exact;

  // If plain integer, also match compound sub-parts (e.g. '1' → '1a', '1b').
  if (/^\d+$/.test(wordParam)) {
    return morphRows.find((row) =>
      String(row['word_position']).toLowerCase().startsWith(lower)
    );
  }

  return undefined;
}

/**
 * Match an English word against the Strong's definition glosses attached to
 * each morphology row.
 *
 * Each morphology row was joined with the strongs table in the initial query,
 * providing a strongs_definition field (the English gloss). We match the
 * user's word as a whole-word, case-insensitive substring of that gloss.
 *
 * Falls back to matching against the lemma field (which may contain an
 * English gloss for rows without a Strong's entry).
 *
 * Returns the first matching row and the total number of matching rows so the
 * caller can surface a matched_count to users when alternatives exist.
 */
function matchByEnglishGloss(
  wordParam: string,
  morphRows: Record<string, unknown>[]
): { first: Record<string, unknown> | undefined; count: number } {
  const target = wordParam.toLowerCase();
  // Build a word-boundary regex so 'sin' doesn't match 'since'.
  const wordBoundaryRe = new RegExp(`\\b${escapeRegex(target)}\\b`, 'i');

  // First pass: exact word-boundary match against Strong's definition gloss.
  const glossMatches = morphRows.filter((row) => {
    const def = row['strongs_definition'];
    if (typeof def === 'string' && def.length > 0) {
      return wordBoundaryRe.test(def);
    }
    return false;
  });
  if (glossMatches.length > 0) {
    return { first: glossMatches[0], count: glossMatches.length };
  }

  // Second pass: word-boundary match against lemma (may be an English gloss
  // for rows where the original script lemma was absent during ETL).
  const lemmaMatches = morphRows.filter((row) => {
    const lemma = row['lemma'];
    if (typeof lemma === 'string' && lemma.length > 0) {
      return wordBoundaryRe.test(lemma);
    }
    return false;
  });
  if (lemmaMatches.length > 0) {
    return { first: lemmaMatches[0], count: lemmaMatches.length };
  }

  // Third pass: substring match against Strong's definition (broader fallback).
  // Guard: only attempt substring matching for inputs of 3+ characters to
  // prevent false positives from short words (e.g. 'in' matching 'beginning').
  if (target.length >= 3) {
    const substringMatches = morphRows.filter((row) => {
      const def = row['strongs_definition'];
      if (typeof def === 'string' && def.length > 0) {
        return def.toLowerCase().includes(target);
      }
      return false;
    });
    if (substringMatches.length > 0) {
      return { first: substringMatches[0], count: substringMatches.length };
    }
  }

  return { first: undefined, count: 0 };
}

/** Escape special regex characters in a literal string. */
function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * Build OtherOccurrence objects directly from inline JOIN results.
 *
 * Rows already contain verse_text and book_name from the batch query JOIN,
 * so no additional D1 round-trip is needed.
 *
 * @param rows              Rows from the batch query (step 7), already joined with verses/books.
 * @param translationAbbrev The abbreviation of the translation used (used in citations).
 */
function buildOtherOccurrencesInline(
  rows: Record<string, unknown>[],
  translationAbbrev: string
): OtherOccurrence[] {
  if (rows.length === 0) return [];

  // Deduplicate by verse reference (the SQL DISTINCT covers morphology rows,
  // but multiple morphology entries for the same verse are possible).
  const seen = new Set<string>();
  const occurrences: OtherOccurrence[] = [];

  for (const row of rows) {
    const bookId = row['book_id'] as number;
    const chapter = row['chapter'] as number;
    const verse = row['verse'] as number;
    const key = `${bookId}.${chapter}.${verse}`;

    if (seen.has(key)) continue;
    seen.add(key);

    const verseText = row['verse_text'] as string | undefined;
    if (!verseText) continue; // verse absent in this translation — skip

    const citation: Citation = {
      book: row['book_name'] as string,
      chapter,
      verse,
      translation: translationAbbrev,
    };

    occurrences.push({
      text: verseText,
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
  translation: T.string({
    required: false,
    description:
      'Translation abbreviation for verse text in results (e.g. "KJV", "WEB", "ASV"). ' +
      'Defaults to KJV when omitted or unrecognized. Does not affect the morphology or ' +
      "Strong's data, which is always Hebrew/Greek.",
  }),
};

export default wordStudy;
