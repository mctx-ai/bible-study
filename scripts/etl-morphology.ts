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
// STEPBible references are formatted as: BookAbbr.Chapter.Verse
// e.g. "Gen.1.1" or "Mat.1.1"
//
// Some data sources also include a word index as a 4th component: "Mat.1.1.01"
// but in TAHOT/TAGNT the word position appears in a separate column (Pos).
// ---------------------------------------------------------------------------

interface ParsedRef {
  book_id: number;
  chapter: number;
  verse: number;
}

function parseRef(ref: string): ParsedRef | null {
  // Handle references with trailing word index component — drop it
  const parts = ref.split('.');
  if (parts.length < 3) return null;

  const bookAbbr = parts[0];
  const chapter = parseInt(parts[1], 10);
  const verse = parseInt(parts[2], 10);

  if (isNaN(chapter) || isNaN(verse)) return null;

  const book_id = STEPBIBLE_BOOK_MAP[bookAbbr];
  if (book_id === undefined) return null;

  return { book_id, chapter, verse };
}

// ---------------------------------------------------------------------------
// Strong's number parser
//
// STEPBible formats:
//   Simple:   H1254   or  G3056
//   Extended: H1254a  (disambiguated sub-entry — strip trailing letters for primary)
//   Compound: H1254a=H1254&H0001  (multi-root — primary is the part before '=')
//
// Rules (decision #7 + #8):
//   strongs_number = primary Strong's number (H/G prefix + 4-digit zero-padded)
//   raw_strongs    = full original string as it appears in the file
// ---------------------------------------------------------------------------

interface ParsedStrongs {
  strongs_number: string | null;
  raw_strongs: string | null;
}

function parseStrongs(raw: string): ParsedStrongs {
  const trimmed = raw.trim();
  if (!trimmed || trimmed === '-') {
    return { strongs_number: null, raw_strongs: null };
  }

  // raw_strongs preserves the full notation exactly
  const raw_strongs = trimmed;

  // Extract primary number — the part before any '=' (for compound notation)
  // then strip trailing disambiguating letters to get the base number.
  // e.g. "H1254a=H1254&H0001" → primary candidate = "H1254a" → "H1254"
  // e.g. "G3056B" → "G3056"
  // e.g. "H0001" → "H0001"
  const primaryCandidate = trimmed.split('=')[0].trim();

  // Match: optional letter prefix, then H or G, then digits, then optional trailing letters
  const match = /^([HG]\d+)[A-Za-z]*$/.exec(primaryCandidate);
  if (!match) {
    // Non-standard format — keep raw, no primary
    return { strongs_number: null, raw_strongs };
  }

  const baseWithPrefix = match[1]; // e.g. "H1254" or "G3056"

  // Normalise to H/G + 4-digit zero-padded (H1254 → H1254, H1 → H0001)
  const prefixMatch = /^([HG])(\d+)$/.exec(baseWithPrefix);
  if (!prefixMatch) {
    return { strongs_number: null, raw_strongs };
  }

  const prefix = prefixMatch[1];
  const digits = prefixMatch[2].padStart(4, '0');
  const strongs_number = `${prefix}${digits}`;

  return { strongs_number, raw_strongs };
}

// ---------------------------------------------------------------------------
// TAHOT / TAGNT file parser
//
// File format (tab-separated, UTF-8):
//   Lines starting with '#' are comments/documentation — skip.
//   The first non-comment line is the column header (starts with 'Ref').
//   Data lines follow the header format.
//
// TAHOT column layout (Hebrew OT):
//   Col 0: Ref          — "Gen.1.1"
//   Col 1: Pos          — word position within verse, may be "1", "1a", "1b"
//   Col 2: Str/dStrong  — Strong's number(s), may be compound: "H1254a=H1254&H0001"
//   Col 3: Morph        — morphology code, e.g. "HVqp3ms"
//   Col 4: Heb          — Hebrew word (original script)
//   Col 5: Translit     — transliteration
//   Col 6: Gloss        — word-level English gloss / translation
//   (additional columns may exist — ignored)
//
// TAGNT column layout (Greek NT):
//   Col 0: Ref          — "Mat.1.1"
//   Col 1: Pos          — word position within verse
//   Col 2: dStrong      — Strong's number(s)
//   Col 3: Morph        — Robinson morphology code, e.g. "N-NSF"
//   Col 4: Grk          — Greek word (original script)
//   Col 5: Translit     — transliteration
//   Col 6: Gloss        — word-level English gloss
//   Col 7: Editions     — manuscript edition attestation codes (TAGNT-specific)
//   (additional columns may exist — ignored)
//
// Both formats share the same column positions for the fields we care about.
// ---------------------------------------------------------------------------

interface ParseFileResult {
  rows: MorphologyRow[];
  skipped: number;
  compoundCount: number;
}

function parseFile(filePath: string, translationId: number): ParseFileResult {
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split('\n');

  const rows: MorphologyRow[] = [];
  let skipped = 0;
  let compoundCount = 0;
  let headerSeen = false;

  // Column indices discovered from header row
  let colRef = 0;
  let colPos = 1;
  let colStrongs = 2;
  let colMorph = 3;
  let colLemma = 4;
  let colGloss = 6;

  for (const rawLine of lines) {
    const line = rawLine.trimEnd();

    // Skip blank lines and comment/documentation lines
    if (!line || line.startsWith('#')) continue;

    const cols = line.split('\t');

    // The header row begins with 'Ref' — use it to discover column positions
    if (!headerSeen && cols[0].trim() === 'Ref') {
      headerSeen = true;
      for (let i = 0; i < cols.length; i++) {
        const h = cols[i].trim().toLowerCase();
        if (h === 'ref') colRef = i;
        else if (h === 'pos' || h === 'word') colPos = i;
        else if (h === 'strongs' || h === 'dstrongs' || h === 'str' || h === 'dstrong') colStrongs = i;
        else if (h === 'morph') colMorph = i;
        else if (h === 'heb' || h === 'grk' || h === 'hebrew' || h === 'greek') colLemma = i;
        else if (h === 'gloss') colGloss = i;
      }
      continue;
    }

    // Need at least the first 5 columns to be useful
    if (cols.length < 5) {
      skipped++;
      continue;
    }

    const refRaw = cols[colRef]?.trim() ?? '';
    const posRaw = cols[colPos]?.trim() ?? '';
    const strongsRaw = cols[colStrongs]?.trim() ?? '';
    const morphRaw = cols[colMorph]?.trim() ?? '';
    const lemmaRaw = cols[colLemma]?.trim() ?? '';
    const glossRaw = cols.length > colGloss ? (cols[colGloss]?.trim() ?? '') : '';

    // Parse reference
    const ref = parseRef(refRaw);
    if (!ref) {
      skipped++;
      continue;
    }

    // Skip lines with no word position (malformed or section markers)
    if (!posRaw || posRaw === '-') {
      skipped++;
      continue;
    }

    // word_position is stored as TEXT — handles "1", "1a", "1b" etc.
    const word_position = posRaw;

    // Parse Strong's
    const { strongs_number, raw_strongs } = parseStrongs(strongsRaw);

    // Count compound Strong's entries for logging
    if (raw_strongs && raw_strongs.includes('=')) {
      compoundCount++;
    }

    // parsing: prefer morphology code; fall back to null if empty/dash
    const parsing = morphRaw && morphRaw !== '-' ? morphRaw : null;

    // lemma: prefer original-script word; fall back to gloss if empty
    const lemma = lemmaRaw && lemmaRaw !== '-' ? lemmaRaw : (glossRaw && glossRaw !== '-' ? glossRaw : null);

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
  const sql = buildMultiRowInserts(
    `INSERT OR IGNORE INTO morphology
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
  console.log('Bible MCP Server — Morphology ETL');
  console.log('===================================\n');

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
