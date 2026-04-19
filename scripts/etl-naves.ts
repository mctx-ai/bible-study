#!/usr/bin/env tsx
/**
 * etl-naves.ts
 *
 * Transforms and loads Nave's Topical Bible data into D1.
 *
 * Data source: data/nave/NavesTopicalDictionary.csv
 * (Downloaded by: npm run data:acquire)
 *
 * Tables populated:
 *   nave_topics       — topic_name, normalized_topic
 *   nave_topic_verses — topic_id, book_id, chapter, verse, note
 *
 * Chapter-only references (decision #5):
 *   Map to verse 1. If verse 1 doesn't exist in verses table, skip and log.
 *   Set note = 'Reference covers broader chapter'.
 *
 * Usage:
 *   npx tsx scripts/etl-naves.ts
 *   npm run etl:naves
 *
 * Required environment variables:
 *   CLOUDFLARE_API_TOKEN
 *   CLOUDFLARE_ACCOUNT_ID
 *   D1_DATABASE_ID
 */

import './load-env.js';
import * as fs from 'fs';
import * as path from 'path';
import { d1 } from '../src/lib/cloudflare.js';
import { d1Etl, buildMultiRowInserts } from '../src/lib/cloudflare-etl.js';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DATA_FILE = path.resolve(
  process.cwd(),
  'data/nave/NavesTopicalDictionary.csv',
);

// ---------------------------------------------------------------------------
// Book abbreviation mapping
// Nave's CSV uses short caps abbreviations; map to book_id matching books table.
// ---------------------------------------------------------------------------

const NAVE_ABBREV_TO_BOOK_ID: Record<string, number> = {
  // Old Testament
  GEN: 1,
  EXO: 2,
  LEV: 3,
  NUM: 4,
  DEU: 5,
  JOS: 6,
  JDG: 7,
  RUT: 8,
  '1SA': 9,
  '2SA': 10,
  '1KI': 11,
  '2KI': 12,
  '1CH': 13,
  '2CH': 14,
  EZR: 15,
  NEH: 16,
  EST: 17,
  JOB: 18,
  PSA: 19,
  PRO: 20,
  ECC: 21,
  // Song of Solomon appears in multiple forms
  SOS: 22,
  SON: 22,
  SOL: 22,
  ISA: 23,
  JER: 24,
  LAM: 25,
  EZK: 26,
  EZE: 26,
  DAN: 27,
  HOS: 28,
  JOL: 29,
  JOE: 29,
  AMO: 30,
  OBA: 31,
  JON: 32,
  MIC: 33,
  NAM: 34,
  HAB: 35,
  ZEP: 36,
  HAG: 37,
  ZEC: 38,
  MAL: 39,
  // New Testament
  MAT: 40,
  MRK: 41,
  MAR: 41,
  LUK: 42,
  JHN: 43,
  JOH: 43,
  ACT: 44,
  ROM: 45,
  '1CO': 46,
  '2CO': 47,
  GAL: 48,
  EPH: 49,
  PHP: 50,
  PHI: 50,
  COL: 51,
  '1TH': 52,
  '2TH': 53,
  '1TI': 54,
  '2TI': 55,
  TIT: 56,
  PHM: 57,
  HEB: 58,
  JAS: 59,
  '1PE': 60,
  '2PE': 61,
  '1JN': 62,
  '1JO': 62,
  '2JN': 63,
  '2JO': 63,
  '3JN': 64,
  '3JO': 64,
  JUD: 65,
  REV: 66,
};

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

function log(msg: string): void {
  console.log(`[etl:naves] ${msg}`);
}

function warn(msg: string): void {
  console.warn(`[etl:naves] WARN  ${msg}`);
}

// ---------------------------------------------------------------------------
// CSV parsing
// ---------------------------------------------------------------------------

interface NaveRow {
  subject: string;
  entry: string;
}

/**
 * Parse the Nave's CSV file.
 * Format: section,subject,entry
 * The entry field is double-quoted and may contain internal commas and newlines.
 *
 * We use a state-machine parser to handle quoted fields correctly.
 */
function parseCsv(filePath: string): NaveRow[] {
  const content = fs.readFileSync(filePath, 'utf-8');
  const rows: NaveRow[] = [];

  // State machine CSV parser
  let inQuote = false;
  let field = '';
  const currentRow: string[] = [];
  let i = 0;

  // Skip BOM if present
  if (content.charCodeAt(0) === 0xfeff) {
    i = 1;
  }

  while (i < content.length) {
    const ch = content[i];

    if (inQuote) {
      if (ch === '"') {
        // Check for escaped quote ""
        if (i + 1 < content.length && content[i + 1] === '"') {
          field += '"';
          i += 2;
        } else {
          // End of quoted field
          inQuote = false;
          i++;
        }
      } else {
        field += ch;
        i++;
      }
    } else {
      if (ch === '"') {
        inQuote = true;
        i++;
      } else if (ch === ',') {
        currentRow.push(field);
        field = '';
        i++;
      } else if (ch === '\n') {
        currentRow.push(field);
        field = '';

        // Process completed row (skip header row "section,subject,entry")
        if (currentRow.length >= 3) {
          const subject = currentRow[1].trim();
          const entry = currentRow[2].trim();
          if (subject && subject !== 'subject') {
            rows.push({ subject, entry });
          }
        }

        currentRow.length = 0;
        i++;
      } else if (ch === '\r') {
        // Skip carriage returns
        i++;
      } else {
        field += ch;
        i++;
      }
    }
  }

  // Handle last row if file doesn't end with newline
  if (field || currentRow.length > 0) {
    currentRow.push(field);
    if (currentRow.length >= 3) {
      const subject = currentRow[1].trim();
      const entry = currentRow[2].trim();
      if (subject && subject !== 'subject') {
        rows.push({ subject, entry });
      }
    }
  }

  return rows;
}

// ---------------------------------------------------------------------------
// Reference parsing
// ---------------------------------------------------------------------------

interface VerseRef {
  bookId: number;
  chapter: number;
  verse: number;
  isChapterOnly: boolean;
}

/**
 * Parse the entry field into individual verse references.
 *
 * Entry format (semicolon-separated references, each prefixed with book abbreviation):
 *   "-Subtopic EXO 6:16-20; JOS 21:4,10; 1CH 6:2,3; 23:13"
 *
 * Reference patterns:
 *   BOOK CHAPTER:VERSE           — single verse
 *   BOOK CHAPTER:VERSE-VERSE     — verse range (expand to individual)
 *   BOOK CHAPTER:VERSE,VERSE     — multiple verses in same chapter
 *   BOOK CHAPTER                 — chapter-only (map to verse 1, set note)
 *   CHAPTER:VERSE                — implicit book (carries forward from last seen book)
 *   CHAPTER                      — implicit book + chapter-only
 */
function parseEntryRefs(entry: string): VerseRef[] {
  const refs: VerseRef[] = [];

  // Strip subtopic labels (lines starting with -)
  // References follow the dash-prefixed subtopic text on the same line.
  // We tokenize by semicolons and parse each segment.
  const segments = entry.split(';');

  let currentBookId: number | null = null;

  for (const rawSegment of segments) {
    const segment = rawSegment.trim();
    if (!segment) continue;

    // Strip leading subtopic label: "-Some label TEXT" or "See TOPIC"
    // The actual reference is usually a BOOK CHAPTER:VERSE pattern.
    // We look for tokens matching known book abbreviations or chapter:verse patterns.
    const refParts = parseSegment(segment);

    for (const part of refParts) {
      if (part.bookId !== null) {
        currentBookId = part.bookId;
      }

      if (currentBookId === null) continue;

      const expandedRefs = expandRef(currentBookId, part);
      refs.push(...expandedRefs);
    }
  }

  return refs;
}

interface ParsedPart {
  bookId: number | null;
  chapter: number | null;
  verseSpec: string | null; // raw verse spec: "1", "1-3", "1,4", null
  isChapterOnly: boolean;
}

/**
 * Parse a single semicolon-delimited segment.
 * A segment may contain a leading subtopic label followed by one or more references.
 * References within a segment are space-separated tokens.
 */
function parseSegment(
  segment: string,
): ParsedPart[] {
  const parts: ParsedPart[] = [];

  // Tokenize by whitespace
  const tokens = segment.split(/\s+/).filter((t) => t.length > 0);

  let i = 0;

  while (i < tokens.length) {
    const token = tokens[i];

    // Check if this token is a book abbreviation
    const bookId = NAVE_ABBREV_TO_BOOK_ID[token.toUpperCase()];
    if (bookId !== undefined) {
      i++;

      // Next token should be a chapter:verse or chapter reference
      if (i < tokens.length) {
        const refToken = tokens[i];
        const chapterVerseResult = parseChapterVerse(refToken);
        if (chapterVerseResult !== null) {
          parts.push({
            bookId: bookId,
            chapter: chapterVerseResult.chapter,
            verseSpec: chapterVerseResult.verseSpec,
            isChapterOnly: chapterVerseResult.isChapterOnly,
          });
          i++;
        } else {
          // Book with no following chapter:verse — skip
          parts.push({
            bookId: bookId,
            chapter: null,
            verseSpec: null,
            isChapterOnly: false,
          });
        }
      }
      continue;
    }

    // Check if this looks like a chapter:verse or chapter continuation
    const chapterVerseResult = parseChapterVerse(token);
    if (chapterVerseResult !== null) {
      parts.push({
        bookId: null, // will inherit book context from parseEntryRefs caller
        chapter: chapterVerseResult.chapter,
        verseSpec: chapterVerseResult.verseSpec,
        isChapterOnly: chapterVerseResult.isChapterOnly,
      });
      i++;
      continue;
    }

    // Otherwise it's a subtopic label word — skip
    i++;
  }

  return parts;
}

interface ChapterVerseResult {
  chapter: number;
  verseSpec: string | null;
  isChapterOnly: boolean;
}

/**
 * Parse a token like "6:16-20", "21:4,10", "6", "23:13".
 * Returns null if token is not a chapter/verse reference.
 */
function parseChapterVerse(token: string): ChapterVerseResult | null {
  // Strip trailing punctuation
  const cleaned = token.replace(/[.,;!?]+$/, '');

  // Pattern: CHAPTER:VERSE_SPEC (e.g., "6:16-20", "21:4,10", "23:13")
  const colonIdx = cleaned.indexOf(':');
  if (colonIdx !== -1) {
    const chapterStr = cleaned.slice(0, colonIdx);
    const verseSpec = cleaned.slice(colonIdx + 1);
    const chapter = parseInt(chapterStr, 10);

    if (isNaN(chapter) || chapter <= 0) return null;
    if (!verseSpec) return null;

    // verseSpec may be "1", "1-3", "1,4,7"
    return { chapter, verseSpec, isChapterOnly: false };
  }

  // Pattern: CHAPTER only (bare integer like "6" or "23")
  const chapter = parseInt(cleaned, 10);
  if (!isNaN(chapter) && chapter > 0 && /^\d+$/.test(cleaned)) {
    return { chapter, verseSpec: null, isChapterOnly: true };
  }

  return null;
}

/**
 * Expand a ParsedPart into zero or more VerseRef entries.
 * Handles verse ranges, comma-separated verses, and chapter-only refs.
 */
function expandRef(bookId: number, part: ParsedPart): VerseRef[] {
  if (part.chapter === null) return [];

  const refs: VerseRef[] = [];
  const chapter = part.chapter;

  if (part.isChapterOnly || part.verseSpec === null) {
    // Chapter-only ref → verse 1 with note
    refs.push({ bookId, chapter, verse: 1, isChapterOnly: true });
    return refs;
  }

  const verseSpec = part.verseSpec;

  // Handle comma-separated specs: "4,10" or "2,3"
  const commaParts = verseSpec.split(',');
  for (const commaPart of commaParts) {
    const rangeParts = commaPart.split('-');
    if (rangeParts.length === 2) {
      const from = parseInt(rangeParts[0], 10);
      const to = parseInt(rangeParts[1], 10);
      if (!isNaN(from) && !isNaN(to) && from > 0 && to >= from) {
        for (let v = from; v <= to; v++) {
          refs.push({ bookId, chapter, verse: v, isChapterOnly: false });
        }
      }
    } else {
      const verse = parseInt(commaPart, 10);
      if (!isNaN(verse) && verse > 0) {
        refs.push({ bookId, chapter, verse, isChapterOnly: false });
      }
    }
  }

  return refs;
}

// ---------------------------------------------------------------------------
// ETL steps
// ---------------------------------------------------------------------------

interface TopicRecord {
  id: number;
  topicName: string;
  normalizedTopic: string;
}

async function loadTopics(rows: NaveRow[]): Promise<Map<string, number>> {
  log('Loading nave_topics...');

  // Deduplicate by normalized name
  const seen = new Map<string, string>(); // normalized → original
  for (const row of rows) {
    const normalized = row.subject.toLowerCase();
    if (!seen.has(normalized)) {
      seen.set(normalized, row.subject);
    }
  }

  const topics: TopicRecord[] = [];
  let id = 1;

  for (const [normalized, original] of seen) {
    topics.push({ id, topicName: original, normalizedTopic: normalized });
    id++;
  }

  const insertRows = topics.map((t) => [t.id, t.topicName, t.normalizedTopic]);
  const sql = buildMultiRowInserts(
    'INSERT OR IGNORE INTO nave_topics (id, topic_name, normalized_topic) VALUES',
    insertRows,
  );
  await d1Etl.batchFile(sql);
  log(`  Inserted ${topics.length} topic records`);

  // Build subject → id map (using original subject from CSV)
  const subjectToId = new Map<string, number>();
  for (const topic of topics) {
    subjectToId.set(topic.topicName, topic.id);
    // Also map normalized → id for lookup
    subjectToId.set(topic.normalizedTopic, topic.id);
  }

  return subjectToId;
}

async function loadTopicVerses(
  rows: NaveRow[],
  subjectToId: Map<string, number>,
  existingVerses: Set<string>,
): Promise<void> {
  log('Loading nave_topic_verses...');

  const insertRows: unknown[][] = [];
  let totalRefs = 0;
  let chapterOnlyCount = 0;
  let skippedNonExistent = 0;
  let skippedUnknownBook = 0;

  // Deduplicate within this run: (topic_id, book_id, chapter, verse)
  const seen = new Set<string>();

  for (const row of rows) {
    const topicId =
      subjectToId.get(row.subject) ??
      subjectToId.get(row.subject.toLowerCase());

    if (topicId === undefined) {
      warn(`No topic_id found for subject: ${row.subject}`);
      continue;
    }

    let refs: VerseRef[];
    try {
      refs = parseEntryRefs(row.entry);
    } catch (err) {
      warn(`Failed to parse entry for topic "${row.subject}": ${String(err)}`);
      continue;
    }

    totalRefs += refs.length;

    for (const ref of refs) {
      // Check for unknown book (shouldn't happen if NAVE_ABBREV_TO_BOOK_ID is complete,
      // but defensive check)
      if (!ref.bookId || ref.bookId < 1 || ref.bookId > 66) {
        skippedUnknownBook++;
        continue;
      }

      // Validate that the verse exists in the verses table
      const verseKey = `${ref.bookId}:${ref.chapter}:${ref.verse}`;
      if (!existingVerses.has(verseKey)) {
        if (ref.isChapterOnly) {
          // Chapter-only ref where verse 1 doesn't exist — skip and log
          warn(
            `Chapter-only ref mapped to verse 1 but ${verseKey} not in verses table — skipping (topic: ${row.subject})`,
          );
        }
        skippedNonExistent++;
        continue;
      }

      // Dedup within this run (D1 UNIQUE constraint handles cross-run dedup)
      const dedupKey = `${topicId}:${verseKey}`;
      if (seen.has(dedupKey)) continue;
      seen.add(dedupKey);

      const note = ref.isChapterOnly ? 'Reference covers broader chapter' : null;

      if (ref.isChapterOnly) {
        chapterOnlyCount++;
      }

      insertRows.push([topicId, ref.bookId, ref.chapter, ref.verse, note]);
    }
  }

  const sql = buildMultiRowInserts(
    'INSERT OR IGNORE INTO nave_topic_verses (topic_id, book_id, chapter, verse, note) VALUES',
    insertRows,
  );
  await d1Etl.batchFile(sql);

  log(`  Total refs parsed:           ${totalRefs.toLocaleString()}`);
  log(`  Verse associations inserted: ${insertRows.length.toLocaleString()}`);
  log(`  Chapter-only refs mapped:    ${chapterOnlyCount.toLocaleString()}`);
  log(`  Skipped (not in verses):     ${skippedNonExistent.toLocaleString()}`);
  if (skippedUnknownBook > 0) {
    log(`  Skipped (unknown book):      ${skippedUnknownBook.toLocaleString()}`);
  }
}

// ---------------------------------------------------------------------------
// Pre-fetch existing verses for validation
// ---------------------------------------------------------------------------

/**
 * Load all verse coordinates from the verses table into memory as a Set.
 * Key format: "book_id:chapter:verse"
 *
 * We only need distinct (book_id, chapter, verse) coordinates regardless of
 * translation, so we query against the KJV translation (id=1) to get the
 * canonical verse set.
 *
 * Fetches in pages to avoid hitting D1 REST response size limits (~31K rows).
 */
async function fetchExistingVerses(): Promise<Set<string>> {
  log('Fetching existing verse coordinates from D1...');

  const verseSet = new Set<string>();
  const pageSize = 10000;
  let offset = 0;

  while (true) {
    const result = await d1.query(
      'SELECT book_id, chapter, verse FROM verses WHERE translation_id = 1 ORDER BY book_id, chapter, verse LIMIT ? OFFSET ?',
      [pageSize, offset],
    );

    for (const row of result.results) {
      const key = `${row['book_id']}:${row['chapter']}:${row['verse']}`;
      verseSet.add(key);
    }

    if (result.results.length < pageSize) break;
    offset += pageSize;
  }

  log(`  Loaded ${verseSet.size.toLocaleString()} verse coordinates`);
  return verseSet;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log("Bible Study MCP Server — Nave's Topical Bible ETL");
  console.log('===========================================\n');

  // Validate required environment variables
  const required = [
    'CLOUDFLARE_API_TOKEN',
    'CLOUDFLARE_ACCOUNT_ID',
    'D1_DATABASE_ID',
  ];
  const missing = required.filter((k) => !process.env[k]);
  if (missing.length > 0) {
    console.error(
      `ERROR: Missing required environment variables: ${missing.join(', ')}`,
    );
    process.exit(1);
  }

  // Verify data file exists
  if (!fs.existsSync(DATA_FILE)) {
    console.error(`ERROR: Data file not found: ${DATA_FILE}`);
    console.error('       Run "npm run data:acquire" first.');
    process.exit(1);
  }

  // Step 1: Parse CSV
  log(`Parsing ${DATA_FILE}...`);
  const rows = parseCsv(DATA_FILE);
  log(`  Parsed ${rows.length.toLocaleString()} rows`);

  // Step 2: Fetch existing verse set for validation
  const existingVerses = await fetchExistingVerses();

  // Step 3: Load topics
  const subjectToId = await loadTopics(rows);

  // Step 4: Load topic-verse associations
  await loadTopicVerses(rows, subjectToId, existingVerses);

  console.log('\n============================================');
  log('ETL complete.');
}

main().catch((err) => {
  console.error('[etl:naves] Unexpected error:', err);
  process.exit(1);
});
