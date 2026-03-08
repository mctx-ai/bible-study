#!/usr/bin/env tsx
/**
 * etl-strongs.ts
 *
 * Transforms and loads Strong's concordance and lexicon data into D1.
 *
 * Sources: STEPBible TBESH (Hebrew) and TBESG (Greek) lexicon files
 * Tables:  strongs, lexicon_entries
 *
 * Usage:
 *   npx tsx scripts/etl-strongs.ts
 *   npm run etl:strongs
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

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface StrongsEntry {
  prefixed_number: string;
  original_word: string;
  transliteration: string;
  definition: string;
  language: 'hebrew' | 'greek';
}

interface LexiconEntry {
  strongs_number: string;
  language: 'hebrew' | 'greek';
  short_def: string;
  long_def: string;
}

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

function log(msg: string): void {
  console.log(`[etl-strongs] ${msg}`);
}

// ---------------------------------------------------------------------------
// TBESH / TBESG file parser
//
// File format: tab-delimited, 8 columns
//   eStrong#  dStrong#  uStrong#  <word>  Transliteration  Morph  Gloss  Meaning
//
// Comment/header lines start with '#' or are the column header row
// (which starts with 'eStrong').
//
// The "Gloss" column is the short definition.
// The "Meaning" column is the long definition (may contain HTML <br> tags).
// ---------------------------------------------------------------------------

interface ParseResult {
  strongs: StrongsEntry[];
  lexicon: LexiconEntry[];
  skipped: number;
}

function parseFile(filePath: string, language: 'hebrew' | 'greek'): ParseResult {
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split('\n');

  const strongs: StrongsEntry[] = [];
  const lexicon: LexiconEntry[] = [];
  let skipped = 0;

  // Track seen prefixed numbers so disambiguated sub-entries (H0001G, H0001H …)
  // are stored only as lexicon entries under the primary number.
  const seenPrimary = new Set<string>();

  for (const rawLine of lines) {
    const line = rawLine.trimEnd();

    // Skip blank lines, comment lines, and the column header line
    if (!line || line.startsWith('#') || line.startsWith('eStrong')) {
      continue;
    }

    const cols = line.split('\t');

    // Need at least 8 columns (eStrong, dStrong, uStrong, word, translit, morph, gloss, meaning)
    if (cols.length < 8) {
      skipped++;
      continue;
    }

    const eStrongRaw = cols[0].trim(); // e.g. "H0001" or "H0001G"
    const wordRaw = cols[3].trim();
    const translitRaw = cols[4].trim();
    const glossRaw = cols[6].trim();
    const meaningRaw = cols[7].trim();

    // Skip empty Strong's numbers
    if (!eStrongRaw) {
      skipped++;
      continue;
    }

    // Determine if this is a primary entry (H0001, G3056) or a disambiguated
    // sub-entry (H0001G, H0001H, G2264A …).
    //
    // Primary entries match exactly: one letter prefix + 4-digit zero-padded number.
    // Sub-entries have a trailing letter (or letters) after the digits.
    const primaryMatch = /^([HG])(\d{4})$/.exec(eStrongRaw);
    const subEntryMatch = /^([HG])(\d{4})[A-Z]+/.exec(eStrongRaw);

    if (primaryMatch) {
      // Primary entry — write to strongs table
      const prefixed_number = eStrongRaw; // already "H0001" or "G3056"

      if (seenPrimary.has(prefixed_number)) {
        // Duplicate — shouldn't happen but guard anyway
        skipped++;
        continue;
      }
      seenPrimary.add(prefixed_number);

      strongs.push({
        prefixed_number,
        original_word: wordRaw,
        transliteration: translitRaw,
        definition: glossRaw,
        language,
      });

      // Also emit a lexicon entry for the primary number
      lexicon.push({
        strongs_number: prefixed_number,
        language,
        short_def: glossRaw,
        long_def: meaningRaw,
      });
    } else if (subEntryMatch) {
      // Disambiguated sub-entry (H0001G = a specific person named "father")
      // Map back to the primary number by stripping the trailing letters.
      const prefix = subEntryMatch[1];
      const digits = subEntryMatch[2];
      const primaryNumber = `${prefix}${digits}`;

      // Emit as a lexicon entry under the primary number so that word_study
      // queries can retrieve all senses for a Strong's number.
      lexicon.push({
        strongs_number: primaryNumber,
        language,
        short_def: glossRaw,
        long_def: meaningRaw,
      });
    } else {
      // Unrecognised format — skip
      skipped++;
    }
  }

  return { strongs, lexicon, skipped };
}

// ---------------------------------------------------------------------------
// ETL steps
// ---------------------------------------------------------------------------

async function loadStrongs(entries: StrongsEntry[]): Promise<void> {
  log(`Inserting ${entries.length} Strong's entries...`);
  const rows = entries.map((e) => [
    e.prefixed_number, e.original_word, e.transliteration, e.definition, e.language,
  ]);
  const sql = buildMultiRowInserts(
    'INSERT OR IGNORE INTO strongs (prefixed_number, original_word, transliteration, definition, language) VALUES',
    rows,
  );
  await d1Etl.batchFile(sql);
}

async function loadLexiconEntries(entries: LexiconEntry[]): Promise<void> {
  log(`Inserting ${entries.length} lexicon entries...`);
  const rows = entries.map((e) => [
    e.strongs_number, e.language, e.short_def, e.long_def,
  ]);
  const sql = buildMultiRowInserts(
    'INSERT OR IGNORE INTO lexicon_entries (strongs_number, language, short_def, long_def) VALUES',
    rows,
    20,
  );
  await d1Etl.batchFile(sql);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log('Bible MCP Server — Strong\'s ETL');
  console.log('=================================\n');

  // Validate required environment variables
  const required = ['CLOUDFLARE_API_TOKEN', 'CLOUDFLARE_ACCOUNT_ID', 'D1_DATABASE_ID'];
  const missing = required.filter((k) => !process.env[k]);
  if (missing.length > 0) {
    console.error(`ERROR: Missing required environment variables: ${missing.join(', ')}`);
    process.exit(1);
  }

  // Validate source files exist
  const tbeshPath = path.join(DATA_DIR, 'TBESH.txt');
  const tbesgPath = path.join(DATA_DIR, 'TBESG.txt');

  for (const [label, filePath] of [['TBESH', tbeshPath], ['TBESG', tbesgPath]] as const) {
    if (!fs.existsSync(filePath)) {
      console.error(`ERROR: ${label} file not found: ${filePath}`);
      console.error('       Run "npm run data:acquire" first.');
      process.exit(1);
    }
  }

  // Parse Hebrew (TBESH)
  log('Parsing TBESH (Hebrew lexicon)...');
  const hebrewResult = parseFile(tbeshPath, 'hebrew');
  log(`  Parsed ${hebrewResult.strongs.length} Hebrew Strong's entries`);
  log(`  Parsed ${hebrewResult.lexicon.length} Hebrew lexicon entries`);
  if (hebrewResult.skipped > 0) {
    log(`  Skipped ${hebrewResult.skipped} unrecognised lines`);
  }

  // Parse Greek (TBESG)
  log('Parsing TBESG (Greek lexicon)...');
  const greekResult = parseFile(tbesgPath, 'greek');
  log(`  Parsed ${greekResult.strongs.length} Greek Strong's entries`);
  log(`  Parsed ${greekResult.lexicon.length} Greek lexicon entries`);
  if (greekResult.skipped > 0) {
    log(`  Skipped ${greekResult.skipped} unrecognised lines`);
  }

  // Combine
  const allStrongs = [...hebrewResult.strongs, ...greekResult.strongs];
  const allLexicon = [...hebrewResult.lexicon, ...greekResult.lexicon];

  // Load to D1
  log('\nLoading to D1...');

  await loadStrongs(allStrongs);
  log(`  Inserted ${hebrewResult.strongs.length} Hebrew + ${greekResult.strongs.length} Greek = ${allStrongs.length} total Strong's entries`);

  await loadLexiconEntries(allLexicon);
  log(`  Inserted ${allLexicon.length} total lexicon entries`);

  // Summary
  console.log('\n=================================');
  log('ETL complete. Counts:');
  log(`  Hebrew Strong's entries:  ${hebrewResult.strongs.length.toLocaleString()}`);
  log(`  Greek Strong's entries:   ${greekResult.strongs.length.toLocaleString()}`);
  log(`  Total Strong's entries:   ${allStrongs.length.toLocaleString()}`);
  log(`  Total lexicon entries:    ${allLexicon.length.toLocaleString()}`);
  log('Done.');
}

main().catch((err) => {
  console.error('[etl-strongs] Unexpected error:', err);
  process.exit(1);
});
