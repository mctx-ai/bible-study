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
  note?: string;
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

  // Resolve translation ID for verse text. Falls back to KJV when the
  // user doesn't specify a translation or specifies an unknown one.
  const kjvTranslation = getTranslation('KJV');
  const kjvId = kjvTranslation?.id;
  if (!kjvId) {
    throw new Error('KJV translation not found in database. Ensure the database is initialized.');
  }
  let verseTranslationId = kjvId;
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

  // 2. Query all morphology rows for this verse, joined with strongs definitions
  //    and lexicon_entries. The strongs JOIN provides English gloss/definition
  //    fields and the lexicon_entries JOIN provides short_def/long_def so that
  //    matchByEnglishGloss can match broader English definitions without
  //    positional alignment.
  //    Morphology rows use translation_id 6 (Hebrew/TAHOT) or 7 (Greek/TAGNT).
  const morphResult = await d1.query(
    `SELECT
       m.id,
       m.word_position,
       m.strongs_number,
       m.lemma,
       m.parsing,
       m.translation_id,
       s.definition AS strongs_definition,
       le.short_def AS lexicon_short_def,
       le.long_def AS lexicon_long_def
     FROM morphology m
     LEFT JOIN strongs s ON s.prefixed_number = m.strongs_number
     LEFT JOIN lexicon_entries le ON le.strongs_number = m.strongs_number
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
  //    Hebrew particles, prefixes, and grammatical markers (definite article he,
  //    conjunctive waw, prepositions beth/kaph/lamed) have null strongs_number.
  //    In that case, return partial results with an explanatory note rather than
  //    throwing, and attempt to find adjacent morphology rows with a valid number.
  const strongsNumber = matchedRow['strongs_number'] as string | null;
  if (!strongsNumber) {
    const position = matchedRow['word_position'];
    const sourceCitation = makeCitation(resolvedBook, chapter, verse, 'ORIG');

    // Search adjacent morphology rows in the same verse for a valid strongs_number.
    const adjacentRow = morphResult.results.find(
      (row) =>
        row['word_position'] !== position &&
        typeof row['strongs_number'] === 'string' &&
        (row['strongs_number'] as string).length > 0
    );

    const note =
      `Word at position ${position} in ${resolvedBook.name} ${chapter}:${verse} ` +
      `is a grammatical particle, prefix, or article (e.g. definite article, ` +
      `conjunctive waw, or prepositional prefix) that does not carry an ` +
      `independent Strong's number. ` +
      (adjacentRow
        ? `The nearest word with a Strong's number is at position ` +
          `${adjacentRow['word_position']} (${adjacentRow['strongs_number']}). ` +
          `Try word_study again with that position for a full result.`
        : `No adjacent words with a Strong's number were found in this verse.`);

    const partialResult: WordStudyResult = {
      original_word: (matchedRow['lemma'] as string) ?? '',
      strongs_number: '',
      transliteration: '',
      definition: '',
      lexicon: { short_def: '', long_def: '' },
      morphology: {
        lemma: (matchedRow['lemma'] as string) ?? '',
        parsing: (matchedRow['parsing'] as string) ?? '',
      },
      matched_count: matchedCount,
      other_occurrences: [],
      total_occurrences: 0,
      citation: sourceCitation,
      note,
    };

    return partialResult;
  }

  // 5–8. Issue all remaining queries concurrently.
  const [strongsResult, lexiconResult, otherVersesMorphResult, countResult] =
    await Promise.all([
      // 5. Strong's entry.
      d1.query(
        `SELECT original_word, transliteration, definition, language
              FROM strongs
              WHERE prefixed_number = ?`,
        [strongsNumber]
      ),
      // 6. Lexicon entry (BDB for Hebrew, Thayer for Greek).
      d1.query(
        `SELECT short_def, long_def
              FROM lexicon_entries
              WHERE strongs_number = ?
              LIMIT 1`,
        [strongsNumber]
      ),
      // 7. Other verses with the same strongs_number (up to 20, canonical order).
      //    JOIN verses and books inline to return verse text and book name
      //    in a single round-trip.
      d1.query(
        `SELECT DISTINCT m.book_id, m.chapter, m.verse,
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
        [verseTranslationId, strongsNumber, resolvedBook.id, chapter, verse]
      ),
      // 8. Total occurrence count (distinct verses).
      //    String concatenation with '.' as separator is safe here because
      //    book_id, chapter, and verse are all integers, so a '.' never
      //    appears in any component value — making each composite key
      //    unambiguous (e.g. "1.2.3" can only mean book 1, chapter 2, verse 3).
      d1.query(
        `SELECT COUNT(DISTINCT (book_id || '.' || chapter || '.' || verse)) AS total
              FROM morphology
              WHERE strongs_number = ?`,
        [strongsNumber]
      ),
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
/** Normalize a word_position string by stripping leading zeros (e.g. '001' → '1', '01a' → '1a'). */
function normalizePos(p: string): string {
  return p.replace(/^0+(\d)/, '$1').toLowerCase();
}

function matchByPosition(
  wordParam: string,
  morphRows: Record<string, unknown>[]
): Record<string, unknown> | undefined {
  const normalizedParam = normalizePos(wordParam);

  // Exact match first (handles '1a', '2b', or plain '1' when only one row).
  const exact = morphRows.find(
    (row) => normalizePos(String(row['word_position'])) === normalizedParam
  );
  if (exact) return exact;

  // If plain integer, also match compound sub-parts (e.g. '1' → '1a', '1b').
  if (/^\d+$/.test(wordParam)) {
    return morphRows.find((row) =>
      normalizePos(String(row['word_position'])).startsWith(normalizedParam)
    );
  }

  return undefined;
}

/**
 * Generate candidate root forms by stripping common English suffixes.
 *
 * Suffixes are tried longest-first to avoid incorrect partial stripping
 * (e.g. '-ieth' before '-eth'). Each rule may also emit a vowel-restored
 * variant (root + 'e') to handle silent-e stems such as 'lov-' → 'love'.
 *
 * The order of candidates returned determines matching priority: more
 * specific (longer suffix stripped) forms come before less specific ones.
 *
 * Note: '-tion' is intentionally excluded — it is a legitimate word ending
 * that should not be stripped.
 */
function generateSuffixCandidates(word: string): string[] {
  const candidates: string[] = [];

  // Helper: add root and optionally root+'e' if not already ending in 'e'.
  function addRoot(root: string, withE = false): void {
    if (root.length === 0) return;
    candidates.push(root);
    if (withE && !root.endsWith('e')) {
      candidates.push(root + 'e');
    }
  }

  // -ieth (e.g. 'fortieth' → 'forti') — strip only, no vowel restore needed.
  if (word.endsWith('ieth')) {
    addRoot(word.slice(0, -4));
  }

  // -eth (e.g. 'giveth' → 'giv', also try 'give').
  if (word.endsWith('eth')) {
    addRoot(word.slice(0, -3), true);
  }

  // -est (e.g. 'greatest' → 'great').
  if (word.endsWith('est')) {
    addRoot(word.slice(0, -3));
  }

  // -ing (e.g. 'loving' → 'lov', also try 'love').
  if (word.endsWith('ing')) {
    addRoot(word.slice(0, -3), true);
  }

  // -ed (e.g. 'loved' → 'lov', also try 'love').
  if (word.endsWith('ed')) {
    addRoot(word.slice(0, -2), true);
  }

  // -es (e.g. 'churches' → 'church').
  if (word.endsWith('es')) {
    addRoot(word.slice(0, -2));
  }

  // -s (e.g. 'sins' → 'sin'). Only strip when the result is ≥3 chars to
  // avoid degenerate roots like 'i' from 'is'.
  if (word.endsWith('s') && !word.endsWith('ss') && word.length > 3) {
    addRoot(word.slice(0, -1));
  }

  // Deduplicate while preserving order.
  const seen = new Set<string>();
  return candidates.filter((c) => {
    if (seen.has(c)) return false;
    seen.add(c);
    return true;
  });
}

/**
 * Match an English word against the Strong's definition glosses and lexicon
 * definitions attached to each morphology row.
 *
 * Each morphology row was joined with the strongs table and lexicon_entries in
 * the initial query, providing strongs_definition (the English gloss),
 * lexicon_short_def, and lexicon_long_def. We match the user's word as a
 * whole-word, case-insensitive substring across all three fields.
 *
 * Falls back to matching against the lemma field (which may contain an
 * English gloss for rows without a Strong's entry).
 *
 * When the original word yields no match, suffix stripping generates
 * candidate root forms (e.g. 'loved' → 'lov', 'love') that are each tried
 * through the same matching passes before giving up.
 *
 * Returns the first matching row and the total number of matching rows so the
 * caller can surface a matched_count to users when alternatives exist.
 */
function matchByEnglishGloss(
  wordParam: string,
  morphRows: Record<string, unknown>[]
): { first: Record<string, unknown> | undefined; count: number } {
  // Inner function: run all matching passes for a given target string.
  // Returns the match result, or { first: undefined, count: 0 } on no match.
  function tryMatch(
    target: string
  ): { first: Record<string, unknown> | undefined; count: number } {
    // Build a word-boundary regex so 'sin' doesn't match 'since'.
    const wordBoundaryRe = new RegExp(`\\b${escapeRegex(target)}\\b`, 'i');

    /**
     * Test whether any of the definition fields on a morphology row match the
     * word-boundary regex. Checks strongs_definition, lexicon_short_def, and
     * lexicon_long_def so that words like "LORD" (gloss) and "Yahweh" (long_def)
     * or inflected forms like "loved"/"love" (long_def expansions) are all reachable.
     */
    function matchesDefinitionFields(row: Record<string, unknown>): boolean {
      const fields = ['strongs_definition', 'lexicon_short_def', 'lexicon_long_def'];
      return fields.some((field) => {
        const val = row[field];
        return typeof val === 'string' && val.length > 0 && wordBoundaryRe.test(val);
      });
    }

    // First pass: word-boundary match against Strong's gloss and lexicon defs.
    const glossMatches = morphRows.filter(matchesDefinitionFields);
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

    // Third pass: substring match against all definition fields (broader fallback).
    // Guard: only attempt substring matching for inputs of 3+ characters to
    // prevent false positives from short words (e.g. 'in' matching 'beginning').
    if (target.length >= 3) {
      const substringMatches = morphRows.filter((row) => {
        const fields = ['strongs_definition', 'lexicon_short_def', 'lexicon_long_def'];
        return fields.some((field) => {
          const val = row[field];
          return typeof val === 'string' && val.length > 0 && val.toLowerCase().includes(target);
        });
      });
      if (substringMatches.length > 0) {
        return { first: substringMatches[0], count: substringMatches.length };
      }
    }

    return { first: undefined, count: 0 };
  }

  const originalTarget = wordParam.toLowerCase();

  // 1. Try original word first (existing behavior).
  const originalResult = tryMatch(originalTarget);
  if (originalResult.first) return originalResult;

  // 2. Generate suffix-stripped candidates and try each in order.
  const candidates = generateSuffixCandidates(originalTarget);
  for (const candidate of candidates) {
    const candidateResult = tryMatch(candidate);
    if (candidateResult.first) return candidateResult;
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

wordStudy.annotations = {
  readOnlyHint: true,
  destructiveHint: false,
  openWorldHint: true,
};

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
