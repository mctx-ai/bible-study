#!/usr/bin/env tsx
/**
 * acquire-data.ts
 *
 * Downloads all raw Bible data sources needed for the ETL pipeline.
 * Downloads are idempotent — existing files are skipped.
 *
 * Usage:
 *   npx tsx scripts/acquire-data.ts
 *   npm run data:acquire
 */

import * as crypto from 'crypto';
import * as fs from 'fs';
import * as https from 'https';
import * as path from 'path';
import { pipeline } from 'stream/promises';
import * as readline from 'readline';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DATA_DIR = path.resolve(process.cwd(), 'data');
const MAX_RETRY_ATTEMPTS = 3;
const RETRY_BASE_DELAY_MS = 2000;

interface DownloadTarget {
  /** Human-readable label for log output */
  label: string;
  /** URL to download from */
  url: string;
  /** Destination path relative to DATA_DIR */
  dest: string;
  /** Expected SHA-256 hex digest (optional — skips checksum verification if absent) */
  sha256?: string;
}

// ---------------------------------------------------------------------------
// Data sources
// ---------------------------------------------------------------------------

/**
 * Bible translations from scrollmapper/bible_databases (CSV format).
 * Raw GitHub URLs pointing to the master branch.
 *
 * NOTE: WEB (World English Bible) is NOT included here because the
 * scrollmapper/bible_databases repository does not provide a WEB.csv file.
 * WEB is acquired separately from alshival/super_bible and converted to
 * scrollmapper format during acquisition — see acquireWEB() below.
 */
const BIBLE_TRANSLATIONS: DownloadTarget[] = [
  {
    label: 'KJV (King James Version)',
    url: 'https://raw.githubusercontent.com/scrollmapper/bible_databases/master/formats/csv/KJV.csv',
    dest: 'scrollmapper/KJV.csv',
  },
  {
    label: 'ASV (American Standard Version)',
    url: 'https://raw.githubusercontent.com/scrollmapper/bible_databases/master/formats/csv/ASV.csv',
    dest: 'scrollmapper/ASV.csv',
  },
  {
    label: "YLT (Young's Literal Translation)",
    url: 'https://raw.githubusercontent.com/scrollmapper/bible_databases/master/formats/csv/YLT.csv',
    dest: 'scrollmapper/YLT.csv',
  },
  {
    label: 'Darby (Darby Translation)',
    url: 'https://raw.githubusercontent.com/scrollmapper/bible_databases/master/formats/csv/Darby.csv',
    dest: 'scrollmapper/Darby.csv',
  },
];

/**
 * STEPBible morphology + lexicon data.
 * TAHOT = Translators Amalgamated Hebrew OT (morphologically tagged)
 * TAGNT = Translators Amalgamated Greek NT (morphologically tagged)
 * TBESH = Brief lexicon of Extended Strongs for Hebrew
 * TBESG = Brief lexicon of Extended Strongs for Greek
 */
const STEPBIBLE_TARGETS: DownloadTarget[] = [
  // Hebrew OT morphology — split across 4 files by canonical section
  {
    label: 'TAHOT Gen-Deu (Hebrew OT morphology)',
    url: 'https://raw.githubusercontent.com/STEPBible/STEPBible-Data/master/Translators%20Amalgamated%20OT+NT/TAHOT%20Gen-Deu%20-%20Translators%20Amalgamated%20Hebrew%20OT%20-%20STEPBible.org%20CC%20BY.txt',
    dest: 'stepbible/TAHOT Gen-Deu.txt',
  },
  {
    label: 'TAHOT Jos-Est (Hebrew OT morphology)',
    url: 'https://raw.githubusercontent.com/STEPBible/STEPBible-Data/master/Translators%20Amalgamated%20OT+NT/TAHOT%20Jos-Est%20-%20Translators%20Amalgamated%20Hebrew%20OT%20-%20STEPBible.org%20CC%20BY.txt',
    dest: 'stepbible/TAHOT Jos-Est.txt',
  },
  {
    label: 'TAHOT Job-Sng (Hebrew OT morphology)',
    url: 'https://raw.githubusercontent.com/STEPBible/STEPBible-Data/master/Translators%20Amalgamated%20OT+NT/TAHOT%20Job-Sng%20-%20Translators%20Amalgamated%20Hebrew%20OT%20-%20STEPBible.org%20CC%20BY.txt',
    dest: 'stepbible/TAHOT Job-Sng.txt',
  },
  {
    label: 'TAHOT Isa-Mal (Hebrew OT morphology)',
    url: 'https://raw.githubusercontent.com/STEPBible/STEPBible-Data/master/Translators%20Amalgamated%20OT+NT/TAHOT%20Isa-Mal%20-%20Translators%20Amalgamated%20Hebrew%20OT%20-%20STEPBible.org%20CC%20BY.txt',
    dest: 'stepbible/TAHOT Isa-Mal.txt',
  },
  // Greek NT morphology — split across 2 files
  {
    label: 'TAGNT Mat-Jhn (Greek NT morphology)',
    url: 'https://raw.githubusercontent.com/STEPBible/STEPBible-Data/master/Translators%20Amalgamated%20OT+NT/TAGNT%20Mat-Jhn%20-%20Translators%20Amalgamated%20Greek%20NT%20-%20STEPBible.org%20CC-BY.txt',
    dest: 'stepbible/TAGNT Mat-Jhn.txt',
  },
  {
    label: 'TAGNT Act-Rev (Greek NT morphology)',
    url: 'https://raw.githubusercontent.com/STEPBible/STEPBible-Data/master/Translators%20Amalgamated%20OT+NT/TAGNT%20Act-Rev%20-%20Translators%20Amalgamated%20Greek%20NT%20-%20STEPBible.org%20CC-BY.txt',
    dest: 'stepbible/TAGNT Act-Rev.txt',
  },
  // Lexicons
  {
    label: 'TBESH (Enhanced Strongs Hebrew lexicon)',
    url: 'https://raw.githubusercontent.com/STEPBible/STEPBible-Data/master/Lexicons/TBESH%20-%20Translators%20Brief%20lexicon%20of%20Extended%20Strongs%20for%20Hebrew%20-%20STEPBible.org%20CC%20BY.txt',
    dest: 'stepbible/TBESH.txt',
  },
  {
    label: 'TBESG (Enhanced Strongs Greek lexicon)',
    url: 'https://raw.githubusercontent.com/STEPBible/STEPBible-Data/master/Lexicons/TBESG%20-%20Translators%20Brief%20lexicon%20of%20Extended%20Strongs%20for%20Greek%20-%20STEPBible.org%20CC%20BY.txt',
    dest: 'stepbible/TBESG.txt',
  },
];

/**
 * OpenBible.info cross-references.
 * The zip archive contains a TSV file with 340,000+ cross-reference pairs.
 */
const CROSS_REFERENCE_TARGETS: DownloadTarget[] = [
  {
    label: 'OpenBible.info cross-references',
    url: 'https://a.openbible.info/data/cross-references.zip',
    dest: 'openbible/cross-references.zip',
  },
];

/**
 * Nave's Topical Bible from bradystephenson/bible-data.
 * The scrollmapper/nave repository does not exist; this is the canonical
 * open-data source for Nave's Topical Dictionary in CSV format.
 */
const NAVE_TARGETS: DownloadTarget[] = [
  {
    label: "Nave's Topical Dictionary (CSV)",
    url: 'https://raw.githubusercontent.com/BradyStephenson/bible-data/main/NavesTopicalDictionary.csv',
    dest: 'nave/NavesTopicalDictionary.csv',
  },
];

/**
 * WEB (World English Bible) translation.
 *
 * The scrollmapper/bible_databases repository does not include WEB. Instead,
 * we source it from alshival/super_bible, which packages WEB in a different
 * CSV format (testament, book, title, chapter, verse, text, version, language)
 * with numeric book IDs and footnote markers in curly braces {like this}.
 *
 * acquireWEB() downloads this source file to a temporary path, converts it to
 * the standard scrollmapper format (Book,Chapter,Verse,Text), strips footnote
 * markers, and writes the result to data/scrollmapper/WEB.csv.
 */
const WEB_SOURCE_URL =
  'https://raw.githubusercontent.com/alshival/super_bible/main/SUPER_BIBLE/version_files/super_bible_WEB.csv';
const WEB_DEST = 'scrollmapper/WEB.csv';
const WEB_TEMP_DEST = 'scrollmapper/.WEB_source.csv';

const ALL_TARGETS: DownloadTarget[] = [
  ...BIBLE_TRANSLATIONS,
  ...STEPBIBLE_TARGETS,
  ...CROSS_REFERENCE_TARGETS,
  ...NAVE_TARGETS,
];

// ---------------------------------------------------------------------------
// Logging helpers
// ---------------------------------------------------------------------------

function log(msg: string): void {
  console.log(`[acquire] ${msg}`);
}

function warn(msg: string): void {
  console.warn(`[acquire] WARN  ${msg}`);
}

function error(msg: string): void {
  console.error(`[acquire] ERROR ${msg}`);
}

// ---------------------------------------------------------------------------
// HTTP download with retry + exponential backoff
// ---------------------------------------------------------------------------

async function httpGetToFile(url: string, dest: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(dest);

    const request = https.get(url, (response) => {
      // Follow redirects (up to 5 hops)
      if (
        response.statusCode !== undefined &&
        response.statusCode >= 300 &&
        response.statusCode < 400 &&
        response.headers.location
      ) {
        file.close();
        fs.unlinkSync(dest);
        httpGetToFile(response.headers.location, dest).then(resolve, reject);
        return;
      }

      if (response.statusCode !== 200) {
        file.close();
        fs.unlinkSync(dest);
        reject(
          new Error(`HTTP ${response.statusCode ?? 'unknown'} for ${url}`),
        );
        return;
      }

      pipeline(response, file)
        .then(resolve)
        .catch((err) => {
          fs.unlinkSync(dest);
          reject(err);
        });
    });

    request.on('error', (err) => {
      file.close();
      if (fs.existsSync(dest)) fs.unlinkSync(dest);
      reject(err);
    });
  });
}

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function downloadWithRetry(url: string, dest: string): Promise<void> {
  let lastError: unknown;

  for (let attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
    try {
      await httpGetToFile(url, dest);
      return;
    } catch (err) {
      lastError = err;
      const isLastAttempt = attempt === MAX_RETRY_ATTEMPTS;
      if (isLastAttempt) break;

      const delay = RETRY_BASE_DELAY_MS * Math.pow(2, attempt - 1);
      const jitter = Math.random() * 0.3 * delay;
      const waitMs = Math.round(delay + jitter);
      warn(
        `Attempt ${attempt}/${MAX_RETRY_ATTEMPTS} failed — retrying in ${waitMs}ms... (${String(err)})`,
      );
      await sleep(waitMs);
    }
  }

  throw lastError;
}

// ---------------------------------------------------------------------------
// Checksum verification
// ---------------------------------------------------------------------------

async function sha256File(filePath: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash('sha256');
    const stream = fs.createReadStream(filePath);
    stream.on('data', (chunk) => hash.update(chunk));
    stream.on('end', () => resolve(hash.digest('hex')));
    stream.on('error', reject);
  });
}

// ---------------------------------------------------------------------------
// Single file acquisition
// ---------------------------------------------------------------------------

async function acquire(target: DownloadTarget): Promise<boolean> {
  const destPath = path.join(DATA_DIR, target.dest);
  const destDir = path.dirname(destPath);

  // Idempotency — skip if file already exists
  if (fs.existsSync(destPath)) {
    log(`SKIP   ${target.label} (already exists: data/${target.dest})`);
    return true;
  }

  fs.mkdirSync(destDir, { recursive: true });

  log(`START  ${target.label}`);
  log(`       -> data/${target.dest}`);

  try {
    await downloadWithRetry(target.url, destPath);

    const bytes = fs.statSync(destPath).size;
    log(`OK     ${target.label} (${(bytes / 1024).toFixed(1)} KB)`);

    if (target.sha256) {
      const actual = await sha256File(destPath);
      if (actual !== target.sha256) {
        error(`Checksum mismatch for ${target.label}`);
        error(`  expected: ${target.sha256}`);
        error(`  actual:   ${actual}`);
        fs.unlinkSync(destPath);
        return false;
      }
      log(`HASH   ${target.label} OK`);
    }

    return true;
  } catch (err) {
    error(`FAIL   ${target.label}: ${String(err)}`);
    if (fs.existsSync(destPath)) fs.unlinkSync(destPath);
    return false;
  }
}

// ---------------------------------------------------------------------------
// WEB (World English Bible) — download + convert from alshival/super_bible
// ---------------------------------------------------------------------------

/**
 * Parses a single CSV line that may contain quoted fields.
 * Handles escaped double-quotes within quoted fields ("" → ").
 */
function parseCSVLine(line: string): string[] {
  const fields: string[] = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];

    if (inQuotes) {
      if (ch === '"') {
        // Peek ahead: "" inside quotes is an escaped quote
        if (i + 1 < line.length && line[i + 1] === '"') {
          current += '"';
          i++; // skip escaped quote
        } else {
          inQuotes = false;
        }
      } else {
        current += ch;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
      } else if (ch === ',') {
        fields.push(current);
        current = '';
      } else {
        current += ch;
      }
    }
  }

  fields.push(current);
  return fields;
}

/**
 * Strips footnote markers of the form {text} from a verse string.
 * The super_bible WEB CSV embeds translator footnotes inline using curly braces.
 */
function stripFootnoteMarkers(text: string): string {
  return text.replace(/\{[^}]*\}/g, '').trim();
}

/**
 * Escapes a field for CSV output — wraps in double-quotes if the value
 * contains a comma, double-quote, or newline.
 */
function csvField(value: string): string {
  if (value.includes(',') || value.includes('"') || value.includes('\n')) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

/**
 * Converts a super_bible WEB CSV (source) to scrollmapper format (output).
 *
 * Source columns: testament,book,title,chapter,verse,text,version,language
 * Output columns: Book,Chapter,Verse,Text
 *
 * The `title` column provides the canonical book name (e.g. "Genesis"), and
 * footnote markers {like this} are stripped from the `text` column.
 */
async function convertSuperBibleToScrollmapper(
  sourcePath: string,
  destPath: string,
): Promise<void> {
  const input = fs.createReadStream(sourcePath);
  const rl = readline.createInterface({ input, crlfDelay: Infinity });

  const lines: string[] = [];
  lines.push('Book,Chapter,Verse,Text');

  let isHeader = true;
  let titleIdx = -1;
  let chapterIdx = -1;
  let verseIdx = -1;
  let textIdx = -1;
  let rowsConverted = 0;

  for await (const line of rl) {
    if (!line.trim()) continue;

    const fields = parseCSVLine(line);

    if (isHeader) {
      // Locate column indices from the header row (case-insensitive)
      const lower = fields.map((f) => f.toLowerCase());
      titleIdx = lower.indexOf('title');
      chapterIdx = lower.indexOf('chapter');
      verseIdx = lower.indexOf('verse');
      textIdx = lower.indexOf('text');

      if (
        titleIdx === -1 ||
        chapterIdx === -1 ||
        verseIdx === -1 ||
        textIdx === -1
      ) {
        throw new Error(
          `super_bible WEB CSV is missing expected columns. Found: ${fields.join(', ')}`,
        );
      }
      isHeader = false;
      continue;
    }

    const book = fields[titleIdx]?.trim() ?? '';
    const chapter = fields[chapterIdx]?.trim() ?? '';
    const verse = fields[verseIdx]?.trim() ?? '';
    const rawText = fields[textIdx]?.trim() ?? '';
    const text = stripFootnoteMarkers(rawText);

    if (!book || !chapter || !verse) continue;

    lines.push(
      `${csvField(book)},${csvField(chapter)},${csvField(verse)},${csvField(text)}`,
    );
    rowsConverted++;
  }

  fs.writeFileSync(destPath, lines.join('\n') + '\n', 'utf8');
  log(`CONVERT WEB: ${rowsConverted} verses written to data/${WEB_DEST}`);
}

/**
 * Acquires the WEB translation from alshival/super_bible and converts it to
 * scrollmapper format, saving the result to data/scrollmapper/WEB.csv.
 *
 * Returns true on success, false on failure.
 */
async function acquireWEB(): Promise<boolean> {
  const destPath = path.join(DATA_DIR, WEB_DEST);
  const tempPath = path.join(DATA_DIR, WEB_TEMP_DEST);
  const destDir = path.dirname(destPath);

  // Idempotency — skip if output file already exists
  if (fs.existsSync(destPath)) {
    log(`SKIP   WEB (World English Bible) (already exists: data/${WEB_DEST})`);
    return true;
  }

  fs.mkdirSync(destDir, { recursive: true });

  log('START  WEB (World English Bible)');
  log(`       source: ${WEB_SOURCE_URL}`);
  log(`       -> data/${WEB_DEST}`);

  try {
    await downloadWithRetry(WEB_SOURCE_URL, tempPath);

    const sourceBytes = fs.statSync(tempPath).size;
    log(`OK     WEB source downloaded (${(sourceBytes / 1024).toFixed(1)} KB)`);

    await convertSuperBibleToScrollmapper(tempPath, destPath);

    const outputBytes = fs.statSync(destPath).size;
    log(`OK     WEB (World English Bible) (${(outputBytes / 1024).toFixed(1)} KB)`);

    return true;
  } catch (err) {
    error(`FAIL   WEB (World English Bible): ${String(err)}`);
    if (fs.existsSync(destPath)) fs.unlinkSync(destPath);
    return false;
  } finally {
    // Always clean up the temp source file
    if (fs.existsSync(tempPath)) fs.unlinkSync(tempPath);
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log('Bible Study MCP Server — Data Acquisition');
  console.log('====================================\n');

  fs.mkdirSync(DATA_DIR, { recursive: true });
  log(`Data directory: ${DATA_DIR}\n`);

  const results = await Promise.allSettled([
    ...ALL_TARGETS.map((target) => acquire(target)),
    acquireWEB(),
  ]);

  const succeeded = results.filter(
    (r) => r.status === 'fulfilled' && r.value,
  ).length;
  const failed = results.length - succeeded;

  console.log('\n=====================================');
  log(`Complete: ${succeeded}/${results.length} succeeded, ${failed} failed`);

  if (failed > 0) {
    error(`${failed} download(s) failed. Re-run to retry.`);
    process.exit(1);
  }
}

main().catch((err) => {
  error(`Unexpected error: ${String(err)}`);
  process.exit(1);
});
