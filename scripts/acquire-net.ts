#!/usr/bin/env tsx
/**
 * acquire-net.ts
 *
 * Downloads the complete NET Bible text from the labs.bible.org API and
 * converts it to scrollmapper CSV format at data/scrollmapper/NET.csv.
 *
 * The NET Bible is copyright Biblical Studies Press, L.L.C.
 * Used for non-commercial purposes under the NET Bible Ministry First license.
 * See https://netbible.com/net-bible-copyright/ for full license terms.
 *
 * Usage:
 *   npx tsx scripts/acquire-net.ts
 *   npm run acquire:net
 */

import * as fs from 'fs';
import * as path from 'path';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DATA_DIR = path.resolve(process.cwd(), 'data');
const NET_CSV_PATH = path.join(DATA_DIR, 'scrollmapper/NET.csv');
const API_BASE = 'http://labs.bible.org/api';
const MAX_RETRY_ATTEMPTS = 3;
const RETRY_BASE_DELAY_MS = 1000;
const REQUEST_DELAY_MS = 300; // polite delay between API calls

// ---------------------------------------------------------------------------
// Book definitions (66 canonical books, KJV order and names)
// Each entry: [canonical name, API passage name, number of chapters]
// ---------------------------------------------------------------------------

interface BookDef {
  /** Canonical KJV name used in scrollmapper CSVs */
  name: string;
  /** Name used in API passage queries */
  apiName: string;
  /** Total number of chapters */
  chapters: number;
}

const BOOKS: BookDef[] = [
  // Old Testament
  { name: 'Genesis', apiName: 'Genesis', chapters: 50 },
  { name: 'Exodus', apiName: 'Exodus', chapters: 40 },
  { name: 'Leviticus', apiName: 'Leviticus', chapters: 27 },
  { name: 'Numbers', apiName: 'Numbers', chapters: 36 },
  { name: 'Deuteronomy', apiName: 'Deuteronomy', chapters: 34 },
  { name: 'Joshua', apiName: 'Joshua', chapters: 24 },
  { name: 'Judges', apiName: 'Judges', chapters: 21 },
  { name: 'Ruth', apiName: 'Ruth', chapters: 4 },
  { name: '1 Samuel', apiName: '1Samuel', chapters: 31 },
  { name: '2 Samuel', apiName: '2Samuel', chapters: 24 },
  { name: '1 Kings', apiName: '1Kings', chapters: 22 },
  { name: '2 Kings', apiName: '2Kings', chapters: 25 },
  { name: '1 Chronicles', apiName: '1Chronicles', chapters: 29 },
  { name: '2 Chronicles', apiName: '2Chronicles', chapters: 36 },
  { name: 'Ezra', apiName: 'Ezra', chapters: 10 },
  { name: 'Nehemiah', apiName: 'Nehemiah', chapters: 13 },
  { name: 'Esther', apiName: 'Esther', chapters: 10 },
  { name: 'Job', apiName: 'Job', chapters: 42 },
  { name: 'Psalms', apiName: 'Psalms', chapters: 150 },
  { name: 'Proverbs', apiName: 'Proverbs', chapters: 31 },
  { name: 'Ecclesiastes', apiName: 'Ecclesiastes', chapters: 12 },
  { name: 'Song of Solomon', apiName: 'Song of Solomon', chapters: 8 },
  { name: 'Isaiah', apiName: 'Isaiah', chapters: 66 },
  { name: 'Jeremiah', apiName: 'Jeremiah', chapters: 52 },
  { name: 'Lamentations', apiName: 'Lamentations', chapters: 5 },
  { name: 'Ezekiel', apiName: 'Ezekiel', chapters: 48 },
  { name: 'Daniel', apiName: 'Daniel', chapters: 12 },
  { name: 'Hosea', apiName: 'Hosea', chapters: 14 },
  { name: 'Joel', apiName: 'Joel', chapters: 3 },
  { name: 'Amos', apiName: 'Amos', chapters: 9 },
  { name: 'Obadiah', apiName: 'Obadiah', chapters: 1 },
  { name: 'Jonah', apiName: 'Jonah', chapters: 4 },
  { name: 'Micah', apiName: 'Micah', chapters: 7 },
  { name: 'Nahum', apiName: 'Nahum', chapters: 3 },
  { name: 'Habakkuk', apiName: 'Habakkuk', chapters: 3 },
  { name: 'Zephaniah', apiName: 'Zephaniah', chapters: 3 },
  { name: 'Haggai', apiName: 'Haggai', chapters: 2 },
  { name: 'Zechariah', apiName: 'Zechariah', chapters: 14 },
  { name: 'Malachi', apiName: 'Malachi', chapters: 4 },
  // New Testament
  { name: 'Matthew', apiName: 'Matthew', chapters: 28 },
  { name: 'Mark', apiName: 'Mark', chapters: 16 },
  { name: 'Luke', apiName: 'Luke', chapters: 24 },
  { name: 'John', apiName: 'John', chapters: 21 },
  { name: 'Acts', apiName: 'Acts', chapters: 28 },
  { name: 'Romans', apiName: 'Romans', chapters: 16 },
  { name: '1 Corinthians', apiName: '1Corinthians', chapters: 16 },
  { name: '2 Corinthians', apiName: '2Corinthians', chapters: 13 },
  { name: 'Galatians', apiName: 'Galatians', chapters: 6 },
  { name: 'Ephesians', apiName: 'Ephesians', chapters: 6 },
  { name: 'Philippians', apiName: 'Philippians', chapters: 4 },
  { name: 'Colossians', apiName: 'Colossians', chapters: 4 },
  { name: '1 Thessalonians', apiName: '1Thessalonians', chapters: 5 },
  { name: '2 Thessalonians', apiName: '2Thessalonians', chapters: 3 },
  { name: '1 Timothy', apiName: '1Timothy', chapters: 6 },
  { name: '2 Timothy', apiName: '2Timothy', chapters: 4 },
  { name: 'Titus', apiName: 'Titus', chapters: 3 },
  { name: 'Philemon', apiName: 'Philemon', chapters: 1 },
  { name: 'Hebrews', apiName: 'Hebrews', chapters: 13 },
  { name: 'James', apiName: 'James', chapters: 5 },
  { name: '1 Peter', apiName: '1Peter', chapters: 5 },
  { name: '2 Peter', apiName: '2Peter', chapters: 3 },
  { name: '1 John', apiName: '1John', chapters: 5 },
  { name: '2 John', apiName: '2John', chapters: 1 },
  { name: '3 John', apiName: '3John', chapters: 1 },
  { name: 'Jude', apiName: 'Jude', chapters: 1 },
  { name: 'Revelation', apiName: 'Revelation', chapters: 22 },
];

// ---------------------------------------------------------------------------
// Logging helpers
// ---------------------------------------------------------------------------

function log(msg: string): void {
  console.log(`[acquire-net] ${msg}`);
}

function warn(msg: string): void {
  console.warn(`[acquire-net] WARN  ${msg}`);
}

function error(msg: string): void {
  console.error(`[acquire-net] ERROR ${msg}`);
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Strips all HTML tags from a string and decodes common HTML entities.
 */
function stripHtml(text: string): string {
  return text
    .replace(/<[^>]*>/g, '') // strip HTML tags
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&nbsp;/gi, ' ')
    .replace(/&#(\d+);/g, (_match, code) => String.fromCharCode(Number(code)))
    .trim();
}

/**
 * Escapes a single CSV field value. Wraps in double-quotes if the value
 * contains a comma, double-quote, or newline character.
 */
function csvField(value: string): string {
  if (value.includes(',') || value.includes('"') || value.includes('\n')) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

// ---------------------------------------------------------------------------
// API types
// ---------------------------------------------------------------------------

interface NetVerse {
  bookname: string;
  chapter: string;
  verse: string;
  text: string;
}

// ---------------------------------------------------------------------------
// HTTP fetch with retry and exponential backoff
// ---------------------------------------------------------------------------

async function fetchWithRetry(url: string): Promise<NetVerse[]> {
  let lastError: unknown;

  for (let attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
    try {
      const response = await fetch(url);

      if (!response.ok) {
        throw new Error(`HTTP ${response.status} ${response.statusText}`);
      }

      const data = await response.json() as NetVerse[];
      return data;
    } catch (err) {
      lastError = err;
      const isLastAttempt = attempt === MAX_RETRY_ATTEMPTS;

      if (isLastAttempt) break;

      const baseDelay = RETRY_BASE_DELAY_MS * Math.pow(2, attempt - 1);
      const jitter = Math.random() * 0.3 * baseDelay;
      const waitMs = Math.round(baseDelay + jitter);

      warn(
        `Attempt ${attempt}/${MAX_RETRY_ATTEMPTS} failed — retrying in ${waitMs}ms... (${String(err)})`,
      );
      await sleep(waitMs);
    }
  }

  throw lastError;
}

// ---------------------------------------------------------------------------
// Chapter fetching
// ---------------------------------------------------------------------------

async function fetchChapter(
  book: BookDef,
  chapter: number,
): Promise<NetVerse[]> {
  const passage = encodeURIComponent(`${book.apiName} ${chapter}`);
  const url = `${API_BASE}/?passage=${passage}&type=json&formatting=plain`;

  const verses = await fetchWithRetry(url);

  if (!Array.isArray(verses) || verses.length === 0) {
    throw new Error(
      `No verses returned for ${book.apiName} ${chapter}`,
    );
  }

  return verses;
}

// ---------------------------------------------------------------------------
// Main acquisition logic
// ---------------------------------------------------------------------------

async function acquireNet(): Promise<void> {
  console.log('Bible MCP Server — NET Bible Acquisition');
  console.log('==========================================\n');

  // Ensure output directory exists
  const outputDir = path.dirname(NET_CSV_PATH);
  fs.mkdirSync(outputDir, { recursive: true });

  // Idempotency — skip if output already exists
  if (fs.existsSync(NET_CSV_PATH)) {
    log(`SKIP   NET.csv already exists at ${NET_CSV_PATH}`);
    log(`       Delete the file and re-run to re-acquire.`);
    return;
  }

  log(`Output: ${NET_CSV_PATH}`);
  log(`Books:  ${BOOKS.length} (39 OT + 27 NT)\n`);

  const rows: string[] = [];
  let totalVerses = 0;
  let totalChapters = 0;

  for (const book of BOOKS) {
    log(`START  ${book.name} (${book.chapters} chapters)`);
    let bookVerses = 0;

    for (let chapter = 1; chapter <= book.chapters; chapter++) {
      let verses: NetVerse[];

      try {
        verses = await fetchChapter(book, chapter);
      } catch (err) {
        error(`Failed to fetch ${book.name} ${chapter}: ${String(err)}`);
        throw err; // Abort — partial output is not useful
      }

      for (const v of verses) {
        const text = stripHtml(v.text);
        rows.push(
          `${csvField(book.name)},${csvField(v.chapter)},${csvField(v.verse)},${csvField(text)}`,
        );
        bookVerses++;
      }

      totalChapters++;

      // Progress indicator for long books
      if (book.chapters > 10) {
        process.stdout.write(
          `\r  [${book.name}] Chapter ${chapter}/${book.chapters} — ${bookVerses} verses`,
        );
      }

      // Polite delay between chapter requests
      await sleep(REQUEST_DELAY_MS);
    }

    if (book.chapters > 10) {
      process.stdout.write('\n');
    }

    totalVerses += bookVerses;
    log(`OK     ${book.name} — ${bookVerses} verses`);
  }

  // Write CSV (no header — scrollmapper format)
  const csvContent = rows.join('\n') + '\n';
  fs.writeFileSync(NET_CSV_PATH, csvContent, 'utf8');

  const bytes = fs.statSync(NET_CSV_PATH).size;
  console.log('\n==========================================');
  log(`Complete: ${BOOKS.length} books, ${totalChapters} chapters, ${totalVerses} verses`);
  log(`Written:  ${NET_CSV_PATH} (${(bytes / 1024).toFixed(1)} KB)`);
}

acquireNet().catch((err) => {
  error(`Unexpected error: ${String(err)}`);
  process.exit(1);
});
