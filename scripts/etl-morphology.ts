#!/usr/bin/env tsx
/**
 * etl-morphology.ts
 *
 * Loads STEPBible Hebrew OT (TAHOT) and Greek NT (TAGNT) morphology data into D1.
 *
 * Sources:
 *   data/stepbible/TAHOT Gen-Deu.txt   — Hebrew Gen through Deuteronomy
 *   data/stepbible/TAHOT Jos-Est.txt   — Hebrew Joshua through Esther
 *   data/stepbible/TAHOT Job-Sng.txt   — Hebrew Job through Song of Solomon
 *   data/stepbible/TAHOT Isa-Mal.txt   — Hebrew Isaiah through Malachi
 *   data/stepbible/TAGNT Mat-Jhn.txt   — Greek Matthew through John
 *   data/stepbible/TAGNT Act-Rev.txt   — Greek Acts through Revelation
 *
 * Compound Strong's (decision #7):
 *   STEPBible notation: H1254a=H1254&H0001
 *   strongs_number stores: H1254  (primary — before the '=')
 *   raw_strongs   stores: H1254a=H1254&H0001  (full compound)
 *
 * word_position is stored as TEXT to handle compound positions like '1a', '1b'.
 *
 * Translation IDs for morphology source texts (separate namespace from English translations):
 *   translation_id 6 — Hebrew OT (TAHOT)
 *   translation_id 7 — Greek NT (TAGNT)
 *
 * Usage:
 *   npx tsx scripts/etl-morphology.ts
 *   npm run etl:morphology
 *
 * Required environment variables:
 *   CLOUDFLARE_API_TOKEN
 *   CLOUDFLARE_ACCOUNT_ID
 *   D1_DATABASE_ID
 */

import './load-env.js';
import * as fs from 'fs';
import * as path from 'path';
import { d1Etl, buildMultiRowInserts } from '../src/lib/cloudflare-etl.js';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DATA_DIR = path.resolve(process.cwd(), 'data/stepbible');

// Source text translation IDs (separate namespace from English translations 1–5)
const TRANSLATION_ID_HEBREW = 6;
const TRANSLATION_ID_GREEK = 7;

// ---------------------------------------------------------------------------
// Book name lookup — maps STEPBible 3-letter abbreviations to our book_id.
// STEPBible uses UBS-style abbreviations (Gen, Exo, Lev, …).
// ---------------------------------------------------------------------------

const STEPBIBLE_BOOK_MAP: Record<string, number> = {
  // Old Testament
  Gen: 1,
  Exo: 2,
  Lev: 3,
  Num: 4,
  Deu: 5,
  Jos: 6,
  Jdg: 7,
  Rut: 8,
  '1Sa': 9,
  '2Sa': 10,
  '1Ki': 11,
  '2Ki': 12,
  '1Ch': 13,
  '2Ch': 14,
  Ezr: 15,
  Neh: 16,
  Est: 17,
  Job: 18,
  Psa: 19,
  Pro: 20,
  Ecc: 21,
  Sng: 22,
  Isa: 23,
  Jer: 24,
  Lam: 25,
  Ezk: 26,
  Dan: 27,
  Hos: 28,
  Jol: 29,
  Amo: 30,
  Oba: 31,
  Jon: 32,
  Mic: 33,
  Nam: 34,
  Hab: 35,
  Zep: 36,
  Hag: 37,
  Zec: 38,
  Mal: 39,
  // New Testament
  Mat: 40,
  Mrk: 41,
  Luk: 42,
  Jhn: 43,
  Act: 44,
  Rom: 45,
  '1Co': 46,
  '2Co': 47,
  Gal: 48,
  Eph: 49,
  Php: 50,
  Col: 51,
  '1Th': 52,
  '2Th': 53,
  '1Ti': 54,
  '2Ti': 55,
  Tit: 56,
  Phm: 57,
  Heb: 58,
  Jas: 59,
  '1Pe': 60,
  '2Pe': 61,
  '1Jn': 62,
  '2Jn': 63,
  '3Jn': 64,
  Jud: 65,
  Rev: 66,
};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface MorphologyRow {
  book_id: number;
  chapter: number;
  verse: number;
  word_position: string;
  strongs_number: string | null;
  raw_strongs: string | null;
  lemma: string | null;
  parsing: string | null;
  translation_id: number;
}

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

function log(msg: string): void {
  console.log(`[etl-morphology] ${msg}`);
}

// ---------------------------------------------------------------------------
// Reference parser
//
// STEPBible references are formatted as: BookAbbr.Chapter.Verse#WordPos=Type
// e.g. "Gen.1.1#01=L" or "Mat.1.1#01=NKO"
//
// The word position is embedded in col 0 after the '#' sign, before the '='.
// e.g. "Mat.1.1#01=NKO" → word_position = "01"
//      "Gen.1.1#01=L"   → word_position = "01"
// ---------------------------------------------------------------------------

interface ParsedRef {
  book_id: number;
  chapter: number;
  verse: number;
  word_position: string | null;
}

function parseRef(ref: string): ParsedRef | null {
  const parts = ref.split('.');
  if (parts.length < 3) return null;

  const bookAbbr = parts[0];
  const chapter = parseInt(parts[1], 10);
  // The third dot-part may contain "#01=NKO" — parseInt stops at '#'
  const verse = parseInt(parts[2], 10);

  if (isNaN(chapter) || isNaN(verse)) return null;

  const book_id = STEPBIBLE_BOOK_MAP[bookAbbr];
  if (book_id === undefined) return null;

  // Extract word position: everything between '#' and '=' (or end of string)
  const hashMatch = /#([^=]+)/.exec(ref);
  const word_position = hashMatch ? hashMatch[1] : null;

  return { book_id, chapter, verse, word_position };
}

// ---------------------------------------------------------------------------
// Strong's number parser
//
// STEPBible formats:
//   Simple:   H1254   or  G3056
//   Extended: H1254a  (disambiguated sub-entry — strip trailing letters for primary)
//   Compound: H1254a=H1254&H0001  (multi-root — primary is the part before '=')
//
// TAGNT format (col 3, dStrongs = Grammar):
//   "G0976=N-NSF"   — Strong's before '=', morphology after '='
//   "G2424G=N-GSM-P" — with trailing letter disambiguator
//
// TAHOT format (col 4, dStrongs):
//   "{H7225G}"           — curly-brace dStrong notation
//   "H9003/{H7225G}"     — compound: prefix Strong's/root Strong's
//   "H9002/H9009/{H0776G}" — multiple prefixes then root
//
// Rules (decision #7 + #8):
//   strongs_number = primary Strong's number (H/G prefix + 4-digit zero-padded)
//   raw_strongs    = full original string as it appears in the file
//
// For TAHOT curly-brace notation, the primary Strong's is:
//   - The Strong's number outside curly braces (leftmost prefix) if present
//   - Otherwise the first Strong's number inside curly braces
//   The root word meaning is typically inside {}, but the prefix preposition/conjunction
//   is the grammatically primary element for the token.
// ---------------------------------------------------------------------------

interface ParsedStrongs {
  strongs_number: string | null;
  raw_strongs: string | null;
}

/**
 * Normalise a raw H/G token to 4-digit zero-padded form.
 * e.g. "H1254a" → "H1254", "G3056B" → "G3056", "H1" → "H0001"
 * Returns null if token does not match expected format.
 */
function normaliseStrongsToken(token: string): string | null {
  // Strip trailing disambiguating letters (e.g. H1254a → H1254, G2424G → G2424)
  const match = /^([HG])(\d+)[A-Za-z]*$/.exec(token.trim());
  if (!match) return null;
  const prefix = match[1];
  const digits = match[2].padStart(4, '0');
  return `${prefix}${digits}`;
}

function parseStrongs(raw: string): ParsedStrongs {
  const trimmed = raw.trim();
  if (!trimmed || trimmed === '-') {
    return { strongs_number: null, raw_strongs: null };
  }

  // raw_strongs preserves the full notation exactly
  const raw_strongs = trimmed;

  // --- TAHOT curly-brace notation ---
  // Formats: "{H7225G}", "H9003/{H7225G}", "H9002/H9009/{H0776G}", etc.
  // The primary Strong's number is:
  //   1. The first token outside curly braces (a prefix like H9003), if one exists
  //   2. Otherwise the first token inside curly braces
  if (trimmed.includes('{')) {
    // Extract first non-curly-brace token (outside {})
    // Tokens are separated by '/' — find the first part that isn't wrapped in {}
    // and doesn't contain a '='
    const segments = trimmed.split('/');
    for (const seg of segments) {
      const s = seg.trim();
      if (!s.startsWith('{') && !s.includes('=')) {
        // Could be a plain Strong's token like H9003
        const normalised = normaliseStrongsToken(s);
        if (normalised) {
          return { strongs_number: normalised, raw_strongs };
        }
      }
    }

    // All tokens are in curly braces — extract first one inside {}
    const curlyMatch = /\{([^}]+)\}/.exec(trimmed);
    if (curlyMatch) {
      // Content inside {} may have trailing suffixes like H7225G — strip them
      const inner = curlyMatch[1].trim();
      const normalised = normaliseStrongsToken(inner);
      if (normalised) {
        return { strongs_number: normalised, raw_strongs };
      }
    }

    // Could not parse — return raw only
    return { strongs_number: null, raw_strongs };
  }

  // --- TAGNT / standard notation ---
  // "G0976=N-NSF" → primary candidate = "G0976" (before '=')
  // "H1254a=H1254&H0001" → primary candidate = "H1254a" → "H1254"
  const primaryCandidate = trimmed.split('=')[0].trim();
  const strongs_number = normaliseStrongsToken(primaryCandidate);
  if (!strongs_number) {
    // Non-standard format — keep raw, no primary
    return { strongs_number: null, raw_strongs };
  }

  return { strongs_number, raw_strongs };
}

// ---------------------------------------------------------------------------
// TAHOT / TAGNT file parser
//
// File format (tab-separated, UTF-8):
//   Lines starting with '#' are comments/documentation — skip.
//   A column header line appears before each book section (repeating headers).
//   Data lines follow the header format.
//
// TAGNT column layout (Greek NT) — header starts with "Word & Type":
//   Col 0: Word & Type       — "Mat.1.1#01=NKO" (ref + word index + edition)
//   Col 1: Greek             — "Βίβλος (Biblos)" Greek word + transliteration
//   Col 2: English translation — "[The] book" English gloss (NOT Strong's!)
//   Col 3: dStrongs = Grammar — "G0976=N-NSF" Strong's number = morphology code
//   Col 4: Dictionary form = Gloss — "βίβλος=book" lemma=gloss
//   Col 5: editions          — "NA28+NA27+..." manuscript editions
//   (additional columns ignored)
//
// TAHOT column layout (Hebrew OT) — header starts with "Eng (Heb) Ref & Type":
//   Col 0: Eng (Heb) Ref & Type — "Gen.1.1#01=L" (ref + word index + type)
//   Col 1: Hebrew              — "בְּ/רֵאשִׁ֖ית" Hebrew word
//   Col 2: Transliteration     — "be./re.Shit" transliteration (NOT Strong's!)
//   Col 3: Translation         — "in/ beginning" English translation
//   Col 4: dStrongs            — "H9003/{H7225G}" Strong's number(s) — curly-brace notation
//   Col 5: Grammar             — "HR/Ncfsa" morphology code
//   Col 6+: variants, root info, etc. (ignored)
//
// Key differences:
//   TAGNT col 3 encodes both Strong's AND morphology as "G0976=N-NSF".
//     parseStrongs extracts "G0976" (before '='), morph stored as full col 3 value.
//   TAHOT col 4 uses curly-brace dStrong notation: "{H7225G}" or "H9003/{H7225G}".
//     parseStrongs handles this notation. Morphology is col 5.
// ---------------------------------------------------------------------------

interface ParseFileResult {
  rows: MorphologyRow[];
  skipped: number;
  compoundCount: number;
}

function parseFile(filePath: string, translationId: number): ParseFileResult {
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split('\n');

  // Detect file type from path for correct column defaults.
  // These are hardcoded from verified file inspection — do not rely on header
  // parsing alone since the header detection was previously broken.
  const isTAGNT = path.basename(filePath).startsWith('TAGNT');

  const rows: MorphologyRow[] = [];
  let skipped = 0;
  let compoundCount = 0;

  // Column indices — set correct defaults per file type.
  //
  // TAGNT (Greek NT): header "Word & Type"
  //   0=Ref+index, 1=Greek word, 2=English gloss, 3=dStrongs=Grammar, 4=lemma=gloss
  //
  // TAHOT (Hebrew OT): header "Eng (Heb) Ref & Type"
  //   0=Ref+index, 1=Hebrew word, 2=transliteration, 3=Translation, 4=dStrongs, 5=Grammar
  //
  // Note: word_position is NOT a column — it is parsed from col 0 (the ref field)
  // after the '#' sign. e.g. "Mat.1.1#01=NKO" → word_position = "01"
  const colRef = 0;
  const colStrongs = isTAGNT ? 3 : 4;   // dStrongs=Grammar (TAGNT) or dStrongs (TAHOT)
  const colMorph = isTAGNT ? 3 : 5;     // same as colStrongs for TAGNT (combined field)
  const colLemma = isTAGNT ? 4 : 1;     // Dictionary form=Gloss (TAGNT) or Hebrew word (TAHOT)

  for (const rawLine of lines) {
    const line = rawLine.trimEnd();

    // Skip blank lines and comment/documentation lines
    if (!line || line.startsWith('#')) continue;

    const cols = line.split('\t');

    // Detect and skip header rows.
    // TAGNT header: cols[0] starts with "Word & Type"
    // TAHOT header: cols[0] starts with "Eng (Heb) Ref & Type"
    const firstCol = cols[0].trim();
    if (firstCol === 'Word & Type' || firstCol === 'Eng (Heb) Ref & Type') {
      continue;
    }

    // Need at least the first 5 columns to be useful
    if (cols.length < 5) {
      skipped++;
      continue;
    }

    const refRaw = cols[colRef]?.trim() ?? '';
    const strongsRaw = cols[colStrongs]?.trim() ?? '';
    const morphRaw = cols[colMorph]?.trim() ?? '';
    const lemmaRaw = cols[colLemma]?.trim() ?? '';

    // Parse reference — word_position is extracted from the '#' fragment in col 0
    const ref = parseRef(refRaw);
    if (!ref) {
      skipped++;
      continue;
    }

    // Skip lines with no word position (malformed or section markers)
    if (!ref.word_position) {
      skipped++;
      continue;
    }

    // word_position is stored as TEXT — handles "01", "1a", "1b" etc.
    const word_position = ref.word_position;

    // Parse Strong's
    const { strongs_number, raw_strongs } = parseStrongs(strongsRaw);

    // Count compound Strong's entries for logging
    if (raw_strongs && raw_strongs.includes('=')) {
      compoundCount++;
    }

    // parsing: prefer morphology code; fall back to null if empty/dash
    const parsing = morphRaw && morphRaw !== '-' ? morphRaw : null;

    // lemma: use column value; null if empty or dash
    const lemma = lemmaRaw && lemmaRaw !== '-' ? lemmaRaw : null;

    rows.push({
      book_id: ref.book_id,
      chapter: ref.chapter,
      verse: ref.verse,
      word_position,
      strongs_number,
      raw_strongs,
      lemma,
      parsing,
      translation_id: translationId,
    });
  }

  return { rows, skipped, compoundCount };
}

// ---------------------------------------------------------------------------
// Ensure source text records exist in translations table
//
// The morphology table has a translation_id foreign-key-style column.
// We register TAHOT and TAGNT as pseudo-translations so the IDs are valid.
// ---------------------------------------------------------------------------

async function ensureSourceTextRecords(): Promise<void> {
  log('Ensuring source text records in translations table...');

  const records = [
    { id: TRANSLATION_ID_HEBREW, abbreviation: 'TAHOT', name: 'Translators Amalgamated Hebrew OT', year: null },
    { id: TRANSLATION_ID_GREEK, abbreviation: 'TAGNT', name: 'Translators Amalgamated Greek NT', year: null },
  ];

  const rows = records.map((r) => [r.id, r.abbreviation, r.name, r.year]);
  const sql = buildMultiRowInserts(
    'INSERT OR IGNORE INTO translations (id, abbreviation, name, year) VALUES',
    rows,
  );
  await d1Etl.batchFile(sql);
}

// ---------------------------------------------------------------------------
// Load morphology rows
// ---------------------------------------------------------------------------

async function loadMorphologyRows(
  morphRows: MorphologyRow[],
  _label: string,
): Promise<void> {
  const rows = morphRows.map((r) => [
    r.book_id,
    r.chapter,
    r.verse,
    r.word_position,
    r.strongs_number,
    r.raw_strongs,
    r.lemma,
    r.parsing,
    r.translation_id,
  ]);
  // INSERT OR REPLACE so that when TAHOT emits both a Ketiv and Qere row for
  // the same (book, chapter, verse, word_position, translation_id), the later
  // row (Qere — the correct reading) overwrites the earlier Ketiv row.
  const sql = buildMultiRowInserts(
    `INSERT OR REPLACE INTO morphology
      (book_id, chapter, verse, word_position, strongs_number, raw_strongs, lemma, parsing, translation_id)
    VALUES`,
    rows,
  );
  await d1Etl.batchFile(sql);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log('Bible Study MCP Server — Morphology ETL');
  console.log('==================================\n');

  // Validate required environment variables
  const required = ['CLOUDFLARE_API_TOKEN', 'CLOUDFLARE_ACCOUNT_ID', 'D1_DATABASE_ID'];
  const missing = required.filter((k) => !process.env[k]);
  if (missing.length > 0) {
    console.error(`ERROR: Missing required environment variables: ${missing.join(', ')}`);
    process.exit(1);
  }

  // ---------------------------------------------------------------------------
  // Source file manifest
  // ---------------------------------------------------------------------------

  const TAHOT_FILES = [
    'TAHOT Gen-Deu.txt',
    'TAHOT Jos-Est.txt',
    'TAHOT Job-Sng.txt',
    'TAHOT Isa-Mal.txt',
  ];

  const TAGNT_FILES = [
    'TAGNT Mat-Jhn.txt',
    'TAGNT Act-Rev.txt',
  ];

  // Validate all source files exist before starting
  const allFiles = [...TAHOT_FILES, ...TAGNT_FILES];
  for (const file of allFiles) {
    const filePath = path.join(DATA_DIR, file);
    if (!fs.existsSync(filePath)) {
      console.error(`ERROR: Source file not found: ${filePath}`);
      console.error('       Run "npm run data:acquire" first.');
      process.exit(1);
    }
  }

  // ---------------------------------------------------------------------------
  // Step 1: Ensure source text translation records exist
  // ---------------------------------------------------------------------------

  await ensureSourceTextRecords();

  // ---------------------------------------------------------------------------
  // Step 2: Parse and load Hebrew OT (TAHOT)
  // ---------------------------------------------------------------------------

  log('\nParsing Hebrew OT (TAHOT) files...');

  let otRows: MorphologyRow[] = [];
  let otSkipped = 0;
  let otCompound = 0;

  for (const file of TAHOT_FILES) {
    const filePath = path.join(DATA_DIR, file);
    log(`  Parsing ${file}...`);
    const result = parseFile(filePath, TRANSLATION_ID_HEBREW);
    log(`    ${result.rows.length.toLocaleString()} rows parsed, ${result.skipped} skipped, ${result.compoundCount} compound Strong's`);
    otRows = otRows.concat(result.rows);
    otSkipped += result.skipped;
    otCompound += result.compoundCount;
  }

  log(`  Hebrew OT total: ${otRows.length.toLocaleString()} words`);
  log(`  Inserting into D1...`);
  await loadMorphologyRows(otRows, 'TAHOT');

  // ---------------------------------------------------------------------------
  // Step 3: Parse and load Greek NT (TAGNT)
  // ---------------------------------------------------------------------------

  log('\nParsing Greek NT (TAGNT) files...');

  let ntRows: MorphologyRow[] = [];
  let ntSkipped = 0;
  let ntCompound = 0;

  for (const file of TAGNT_FILES) {
    const filePath = path.join(DATA_DIR, file);
    log(`  Parsing ${file}...`);
    const result = parseFile(filePath, TRANSLATION_ID_GREEK);
    log(`    ${result.rows.length.toLocaleString()} rows parsed, ${result.skipped} skipped, ${result.compoundCount} compound Strong's`);
    ntRows = ntRows.concat(result.rows);
    ntSkipped += result.skipped;
    ntCompound += result.compoundCount;
  }

  log(`  Greek NT total: ${ntRows.length.toLocaleString()} words`);
  log(`  Inserting into D1...`);
  await loadMorphologyRows(ntRows, 'TAGNT');

  // ---------------------------------------------------------------------------
  // Step 4: Summary
  // ---------------------------------------------------------------------------

  const totalLoaded = otRows.length + ntRows.length;
  const totalCompound = otCompound + ntCompound;
  const totalSkipped = otSkipped + ntSkipped;

  console.log('\n===================================');
  log('ETL complete. Counts:');
  log(`  OT words (Hebrew):         ${otRows.length.toLocaleString()}`);
  log(`  NT words (Greek):          ${ntRows.length.toLocaleString()}`);
  log(`  Compound Strong's entries: ${totalCompound.toLocaleString()}`);
  log(`  Total loaded:              ${totalLoaded.toLocaleString()}`);
  log(`  Lines skipped:             ${totalSkipped.toLocaleString()}`);
  log('Done.');
}

main().catch((err) => {
  console.error('[etl-morphology] Unexpected error:', err);
  process.exit(1);
});
