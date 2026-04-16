#!/usr/bin/env tsx
/**
 * etl-crossrefs.ts
 *
 * Loads OpenBible.info cross-reference data into the D1 cross_references table.
 *
 * Data source: data/openbible/cross-references.zip (downloaded by acquire-data.ts)
 * The zip contains a single TSV file with ~340K cross-reference pairs.
 *
 * TSV format (tab-separated, with header row):
 *   From Verse  To Verse    Votes
 *   Gen.1.1     Joh.1.1-3   42
 *
 * Processing steps:
 *   1. Unzip and parse the TSV file
 *   2. Build a book-name-to-book_id lookup from the D1 books table
 *   3. Build a verse existence set from the D1 verses table (KJV only)
 *   4. For each pair, expand verse ranges in the "To Verse" field to individual rows
 *   5. Resolve book names to book_id for both from and to verses
 *   6. Validate that target verses exist in the verses table; skip dangling refs
 *   7. Insert into cross_references with INSERT OR IGNORE (idempotent)
 *   8. Bulk insert via d1Etl.batchFile (single wrangler call for all rows)
 *
 * Usage:
 *   npx tsx scripts/etl-crossrefs.ts
 *   npm run etl:crossrefs
 *
 * Required environment variables:
 *   CLOUDFLARE_API_TOKEN
 *   CLOUDFLARE_ACCOUNT_ID
 *   D1_DATABASE_ID
 */

import './load-env.js';
import * as fs from 'fs';
import * as path from 'path';
import * as zlib from 'zlib';
import { d1 } from '../src/lib/cloudflare.js';
import { d1Etl, buildMultiRowInserts } from '../src/lib/cloudflare-etl.js';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DATA_DIR = path.resolve(process.cwd(), 'data/openbible');
const ZIP_FILE = path.join(DATA_DIR, 'cross-references.zip');
const SOURCE = 'openbible';

// KJV translation_id in the translations table (loaded by etl-bible-text.ts)
const KJV_TRANSLATION_ID = 1;

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

function log(msg: string): void {
  console.log(`[etl:crossrefs] ${msg}`);
}

function warn(msg: string): void {
  console.warn(`[etl:crossrefs] WARN  ${msg}`);
}

// ---------------------------------------------------------------------------
// Zip extraction (pure Node.js, no shell commands)
// ---------------------------------------------------------------------------

/**
 * Read the contents of the first .txt or .tsv file inside a zip archive.
 *
 * The zip file format:
 *   - Local file header: PK\x03\x04 + metadata + filename + data
 *   - Each entry is self-contained; we walk through the archive sequentially.
 *
 * This is a minimal zip reader sufficient for a single-entry archive.
 * It handles stored (method 0) and deflated (method 8) compression.
 */
function extractFirstTextFileFromZip(zipPath: string): string {
  const buf = fs.readFileSync(zipPath);
  let offset = 0;

  while (offset < buf.length - 4) {
    // Local file header signature: 0x04034b50 (little-endian)
    if (
      buf[offset] !== 0x50 ||
      buf[offset + 1] !== 0x4b ||
      buf[offset + 2] !== 0x03 ||
      buf[offset + 3] !== 0x04
    ) {
      // Not a local file header — skip to end-of-central-directory or bail
      break;
    }

    const compressionMethod = buf.readUInt16LE(offset + 8);
    const compressedSize = buf.readUInt32LE(offset + 18);
    const filenameLength = buf.readUInt16LE(offset + 26);
    const extraFieldLength = buf.readUInt16LE(offset + 28);

    const filename = buf
      .slice(offset + 30, offset + 30 + filenameLength)
      .toString('utf-8');
    const dataOffset = offset + 30 + filenameLength + extraFieldLength;
    const compressedData = buf.slice(dataOffset, dataOffset + compressedSize);

    const lowerName = filename.toLowerCase();
    if (lowerName.endsWith('.txt') || lowerName.endsWith('.tsv')) {
      log(`Found data file in zip: ${filename}`);

      if (compressionMethod === 0) {
        // Stored (no compression)
        return compressedData.toString('utf-8');
      } else if (compressionMethod === 8) {
        // Deflated
        const decompressed = zlib.inflateRawSync(compressedData);
        return decompressed.toString('utf-8');
      } else {
        throw new Error(
          `Unsupported zip compression method ${compressionMethod} for ${filename}`,
        );
      }
    }

    offset = dataOffset + compressedSize;
  }

  throw new Error(
    `No .txt or .tsv file found in zip archive: ${zipPath}`,
  );
}

// ---------------------------------------------------------------------------
// TSV parsing
// ---------------------------------------------------------------------------

interface RawCrossRef {
  fromVerse: string; // e.g., "Gen.1.1"
  toVerse: string;   // e.g., "Joh.1.1" or "Rom.8.28-Rom.8.30"
  votes: number;     // raw vote count from OpenBible
}

/**
 * Parse the OpenBible TSV.
 *
 * Header: "From Verse\tTo Verse\tVotes"
 * Verse format: Book.Chapter.Verse (e.g., Gen.1.1, Rom.8.28)
 * Range format: Book.Chapter.VerseStart-Book.Chapter.VerseEnd (e.g., Rom.8.28-Rom.8.30)
 * Also possible: Book.Chapter.VerseStart-VerseEnd (short form, e.g., Rom.8.28-30)
 */
function parseTsv(content: string): RawCrossRef[] {
  const lines = content.split('\n');
  const results: RawCrossRef[] = [];

  // Skip header row
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    const parts = line.split('\t');
    if (parts.length < 3) {
      warn(`Skipping malformed TSV line ${i + 1}: ${line.slice(0, 80)}`);
      continue;
    }

    const fromVerse = parts[0].trim();
    const toVerse = parts[1].trim();
    const votesStr = parts[2].trim();
    const votes = parseInt(votesStr, 10);

    if (!fromVerse || !toVerse) {
      warn(`Skipping empty verse reference at line ${i + 1}`);
      continue;
    }

    results.push({ fromVerse, toVerse, votes: isNaN(votes) ? 0 : votes });
  }

  return results;
}

// ---------------------------------------------------------------------------
// Verse reference parsing and range expansion
// ---------------------------------------------------------------------------

interface VerseRef {
  bookAbbr: string; // e.g., "Gen", "Rom", "1Sam"
  chapter: number;
  verse: number;
}

/**
 * Parse an OpenBible verse token like "Gen.1.1" or "1Sam.2.3".
 * Returns null if the token is not parseable.
 */
function parseVerseToken(token: string): VerseRef | null {
  // Format: BookAbbr.Chapter.Verse
  // BookAbbr may contain digits (1Sam, 2Kgs, etc.)
  // We split on '.' but the book part may have dots removed — OpenBible uses
  // single abbreviated forms without dots in the book name.
  const parts = token.split('.');
  if (parts.length < 3) return null;

  // The book abbreviation is everything up to the second-to-last dot
  // For "1Sam.2.3" -> parts = ["1Sam", "2", "3"] -> bookAbbr = "1Sam"
  // For "Song.1.1" -> parts = ["Song", "1", "1"]
  // The last two parts are chapter and verse
  const verseNum = parseInt(parts[parts.length - 1], 10);
  const chapterNum = parseInt(parts[parts.length - 2], 10);
  const bookAbbr = parts.slice(0, parts.length - 2).join('.');

  if (isNaN(chapterNum) || isNaN(verseNum) || !bookAbbr) return null;

  return { bookAbbr, chapter: chapterNum, verse: verseNum };
}

/**
 * Expand a "To Verse" field that may contain a range into individual VerseRef objects.
 *
 * Examples:
 *   "Gen.1.1"           -> [{ bookAbbr: "Gen", chapter: 1, verse: 1 }]
 *   "Rom.8.28-Rom.8.30" -> [{ bookAbbr: "Rom", chapter: 8, verse: 28 },
 *                            { bookAbbr: "Rom", chapter: 8, verse: 29 },
 *                            { bookAbbr: "Rom", chapter: 8, verse: 30 }]
 *   "Rom.8.28-30"       -> same as above (short-form range, same book+chapter)
 *
 * Returns an empty array if the reference cannot be parsed.
 */
function expandVerseRange(
  toVerse: string,
  stats: EtlStats,
): VerseRef[] {
  // Check if there's a range separator '-'
  const dashIndex = toVerse.indexOf('-');

  if (dashIndex === -1) {
    // Single verse
    const ref = parseVerseToken(toVerse);
    if (!ref) {
      warn(`Cannot parse verse reference: ${toVerse}`);
      return [];
    }
    return [ref];
  }

  // Range: split at '-' to get start and end
  const startToken = toVerse.slice(0, dashIndex);
  const endToken = toVerse.slice(dashIndex + 1);

  const startRef = parseVerseToken(startToken);
  if (!startRef) {
    warn(`Cannot parse start of verse range: ${toVerse}`);
    return [];
  }

  // End token may be a full reference (Book.Chapter.Verse) or just a verse number
  let endRef: VerseRef | null = null;

  if (endToken.includes('.')) {
    // Full reference like "Rom.8.30"
    endRef = parseVerseToken(endToken);
  } else {
    // Short form: just a verse number, same book+chapter as start
    const endVerse = parseInt(endToken, 10);
    if (!isNaN(endVerse)) {
      endRef = {
        bookAbbr: startRef.bookAbbr,
        chapter: startRef.chapter,
        verse: endVerse,
      };
    }
  }

  if (!endRef) {
    warn(`Cannot parse end of verse range: ${toVerse}`);
    return [];
  }

  // Validate range: must be same book and chapter, end >= start
  if (
    endRef.bookAbbr !== startRef.bookAbbr ||
    endRef.chapter !== startRef.chapter
  ) {
    // Cross-chapter ranges are rare and complex; treat as single start verse
    warn(
      `Cross-chapter/book range not supported, using start only: ${toVerse}`,
    );
    return [startRef];
  }

  if (endRef.verse < startRef.verse) {
    warn(`Range end < start, using start only: ${toVerse}`);
    return [startRef];
  }

  // Expand the range
  const expanded: VerseRef[] = [];
  for (let v = startRef.verse; v <= endRef.verse; v++) {
    expanded.push({
      bookAbbr: startRef.bookAbbr,
      chapter: startRef.chapter,
      verse: v,
    });
  }

  stats.rangesExpanded++;
  return expanded;
}

// ---------------------------------------------------------------------------
// D1 lookup helpers
// ---------------------------------------------------------------------------

/**
 * Build a map from OpenBible book abbreviation to book_id by querying
 * both the books table (abbreviation column) and book_aliases table.
 *
 * OpenBible uses abbreviations like: Gen, Exod, Lev, Num, Deut, Josh,
 * Judg, Ruth, 1Sam, 2Sam, 1Kgs, 2Kgs, 1Chr, 2Chr, Ezra, Neh, Esth,
 * Job, Ps, Prov, Eccl, Song, Isa, Jer, Lam, Ezek, Dan, Hos, Joel,
 * Amos, Obad, Jonah, Mic, Nah, Hab, Zeph, Hag, Zech, Mal,
 * Matt, Mark, Luke, John, Acts, Rom, 1Cor, 2Cor, Gal, Eph, Phil,
 * Col, 1Thess, 2Thess, 1Tim, 2Tim, Titus, Phlm, Heb, Jas, 1Pet,
 * 2Pet, 1John, 2John, 3John, Jude, Rev
 */
async function buildBookLookup(): Promise<Map<string, number>> {
  log('Building book lookup from D1 books and book_aliases tables...');

  // Query both tables in one round trip using UNION
  const result = await d1.query(`
    SELECT abbreviation AS alias, id AS book_id FROM books
    UNION ALL
    SELECT alias, book_id FROM book_aliases
  `);

  const map = new Map<string, number>();
  for (const row of result.results) {
    const alias = row['alias'] as string;
    const bookId = row['book_id'] as number;
    if (alias && bookId) {
      map.set(alias, bookId);
    }
  }

  log(`  Built lookup with ${map.size} entries covering ${new Set(map.values()).size} books`);
  return map;
}

/**
 * Build a Set of "bookId:chapter:verse" keys for all verses in the KJV translation.
 * Used to validate that target verses exist before inserting cross-references.
 *
 * Fetches in pages to avoid D1 result size limits.
 */
async function buildVerseSet(): Promise<Set<string>> {
  log('Building verse existence set from D1 verses table (KJV)...');

  const verseSet = new Set<string>();
  const pageSize = 10000;
  let offset = 0;

  while (true) {
    const result = await d1.query(
      `SELECT book_id, chapter, verse FROM verses
       WHERE translation_id = ?
       LIMIT ? OFFSET ?`,
      [KJV_TRANSLATION_ID, pageSize, offset],
    );

    for (const row of result.results) {
      const key = `${row['book_id']}:${row['chapter']}:${row['verse']}`;
      verseSet.add(key);
    }

    if (result.results.length < pageSize) break;
    offset += pageSize;

    if (offset % 50000 === 0) {
      log(`  Loaded ${offset.toLocaleString()} verses so far...`);
    }
  }

  log(`  Verse set built: ${verseSet.size.toLocaleString()} KJV verses`);
  return verseSet;
}

// ---------------------------------------------------------------------------
// Stats tracking
// ---------------------------------------------------------------------------

interface EtlStats {
  totalPairs: number;
  rangesExpanded: number;          // number of pairs that were ranges (not single verses)
  danglingSkipped: number;         // TO verses not found in the verses table
  fromDanglingSkipped: number;     // FROM verses not found in the verses table
  bookNotFound: number;            // references where book abbreviation couldn't be resolved
  insertedRows: number;            // rows successfully queued for INSERT
}

// ---------------------------------------------------------------------------
// OpenBible book abbreviation normalization
// ---------------------------------------------------------------------------

/**
 * OpenBible uses a specific set of book abbreviations that differ slightly
 * from the ones stored in our books/book_aliases tables.
 *
 * This map translates OpenBible abbreviations to the ones in our DB.
 * Most match directly (Gen, Exod, Lev, etc.) but a few differ.
 */
const OPENBIBLE_ABBR_MAP: Record<string, string> = {
  // OpenBible -> our DB alias
  Exod: 'Exod',
  Lev: 'Lev',
  Num: 'Num',
  Deut: 'Deut',
  Josh: 'Josh',
  Judg: 'Judg',
  '1Sam': '1Sam',
  '2Sam': '2Sam',
  '1Kgs': '1Kgs',
  '2Kgs': '2Kgs',
  '1Chr': '1Chr',
  '2Chr': '2Chr',
  Ezra: 'Ezra',
  Neh: 'Neh',
  Esth: 'Esth',
  Ps: 'Ps',
  Prov: 'Prov',
  Eccl: 'Eccl',
  Song: 'Song',
  Isa: 'Isa',
  Jer: 'Jer',
  Lam: 'Lam',
  Ezek: 'Ezek',
  Dan: 'Dan',
  Hos: 'Hos',
  Amos: 'Amos',
  Obad: 'Obad',
  Jonah: 'Jonah',
  Mic: 'Mic',
  Nah: 'Nah',
  Hab: 'Hab',
  Zeph: 'Zeph',
  Hag: 'Hag',
  Zech: 'Zech',
  Mal: 'Mal',
  Matt: 'Matt',
  Mark: 'Mark',
  Luke: 'Luke',
  John: 'John',
  Acts: 'Acts',
  Rom: 'Rom',
  '1Cor': '1Cor',
  '2Cor': '2Cor',
  Gal: 'Gal',
  Eph: 'Eph',
  Phil: 'Phil',
  Col: 'Col',
  '1Thess': '1Thess',
  '2Thess': '2Thess',
  '1Tim': '1Tim',
  '2Tim': '2Tim',
  Titus: 'Titus',
  Phlm: 'Phlm',
  Heb: 'Heb',
  Jas: 'Jas',
  '1Pet': '1Pet',
  '2Pet': '2Pet',
  '1John': '1John',
  '2John': '2John',
  '3John': '3John',
  Jude: 'Jude',
  Rev: 'Rev',
};

/**
 * Resolve an OpenBible book abbreviation to a book_id.
 * Tries the abbreviation directly, then checks OPENBIBLE_ABBR_MAP,
 * then falls back to direct map lookup.
 */
function resolveBookId(
  bookAbbr: string,
  bookLookup: Map<string, number>,
): number | null {
  // Direct lookup
  if (bookLookup.has(bookAbbr)) {
    return bookLookup.get(bookAbbr)!;
  }

  // Try normalized abbreviation via OPENBIBLE_ABBR_MAP
  const normalized = OPENBIBLE_ABBR_MAP[bookAbbr];
  if (normalized && bookLookup.has(normalized)) {
    return bookLookup.get(normalized)!;
  }

  return null;
}

// ---------------------------------------------------------------------------
// Normalize confidence from votes
// ---------------------------------------------------------------------------

/**
 * OpenBible "Votes" are crowd-sourced vote counts, not probabilities.
 * We store them as-is in the confidence column (REAL type).
 * The maximum vote count in the dataset is roughly 1000+, so we preserve
 * raw values rather than normalizing — callers can rank by confidence.
 */
function votesToConfidence(votes: number): number {
  return votes;
}

// ---------------------------------------------------------------------------
// Main ETL function
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log('Bible Study MCP Server — Cross-Reference ETL');
  console.log('======================================\n');

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
  if (!fs.existsSync(ZIP_FILE)) {
    log(`ERROR: Data file not found: ${ZIP_FILE}`);
    log(`       Run "npm run data:acquire" first.`);
    process.exit(1);
  }

  // Step 1: Extract and parse TSV from zip
  log('Extracting cross-references zip...');
  const tsvContent = extractFirstTextFileFromZip(ZIP_FILE);

  log('Parsing TSV...');
  const rawRefs = parseTsv(tsvContent);
  log(`  Parsed ${rawRefs.length.toLocaleString()} raw cross-reference pairs`);

  // Step 2: Build lookups from D1
  const bookLookup = await buildBookLookup();
  const verseSet = await buildVerseSet();

  // Step 3: Process all pairs, expand ranges, resolve IDs, validate verses
  log('\nProcessing cross-reference pairs...');

  const stats: EtlStats = {
    totalPairs: rawRefs.length,
    rangesExpanded: 0,
    danglingSkipped: 0,
    fromDanglingSkipped: 0,
    bookNotFound: 0,
    insertedRows: 0,
  };

  const rows: unknown[][] = [];

  const insertPrefix = `INSERT OR IGNORE INTO cross_references
    (from_book_id, from_chapter, from_verse, to_book_id, to_chapter, to_verse, source, confidence)
  VALUES`;

  for (const raw of rawRefs) {
    // Parse "From Verse"
    const fromRef = parseVerseToken(raw.fromVerse);
    if (!fromRef) {
      warn(`Cannot parse from-verse: ${raw.fromVerse}`);
      stats.bookNotFound++;
      continue;
    }

    const fromBookId = resolveBookId(fromRef.bookAbbr, bookLookup);
    if (fromBookId === null) {
      warn(`Unknown from-book abbreviation: "${fromRef.bookAbbr}" (${raw.fromVerse})`);
      stats.bookNotFound++;
      continue;
    }

    // Validate that the source verse exists in the verses table
    const fromVerseKey = `${fromBookId}:${fromRef.chapter}:${fromRef.verse}`;
    if (!verseSet.has(fromVerseKey)) {
      stats.fromDanglingSkipped++;
      continue;
    }

    // Expand "To Verse" (may be a range)
    const toRefs = expandVerseRange(raw.toVerse, stats);
    if (toRefs.length === 0) continue;

    const confidence = votesToConfidence(raw.votes);

    for (const toRef of toRefs) {
      const toBookId = resolveBookId(toRef.bookAbbr, bookLookup);
      if (toBookId === null) {
        warn(`Unknown to-book abbreviation: "${toRef.bookAbbr}" (${raw.toVerse})`);
        stats.bookNotFound++;
        continue;
      }

      // Validate that the target verse exists in the verses table
      const verseKey = `${toBookId}:${toRef.chapter}:${toRef.verse}`;
      if (!verseSet.has(verseKey)) {
        stats.danglingSkipped++;
        continue;
      }

      rows.push([
        fromBookId,
        fromRef.chapter,
        fromRef.verse,
        toBookId,
        toRef.chapter,
        toRef.verse,
        SOURCE,
        confidence,
      ]);

      stats.insertedRows++;
    }
  }

  log(
    `\nProcessing complete:` +
    `\n  Total raw pairs:      ${stats.totalPairs.toLocaleString()}` +
    `\n  Ranges expanded:      ${stats.rangesExpanded.toLocaleString()} pairs were ranges` +
    `\n  Book not found:       ${stats.bookNotFound.toLocaleString()} (skipped)` +
    `\n  From dangling:        ${stats.fromDanglingSkipped.toLocaleString()} (source verse not in verses table)` +
    `\n  To dangling:          ${stats.danglingSkipped.toLocaleString()} (target verse not in verses table)` +
    `\n  Rows to insert:       ${stats.insertedRows.toLocaleString()}`,
  );

  // Step 4: Delete existing rows for this source (idempotency)
  // The cross_references table has no UNIQUE constraint on the data columns,
  // so INSERT OR IGNORE alone cannot prevent duplicates on re-runs.
  // We delete all existing rows from this source before re-inserting.
  log(`\nDeleting existing rows for source='${SOURCE}' (idempotent re-run support)...`);
  const deleteResult = await d1.query(
    `DELETE FROM cross_references WHERE source = ?`,
    [SOURCE],
  );
  log(`  Deleted ${deleteResult.meta.changes} existing rows`);

  // Step 5: Insert all rows in a single wrangler call
  if (rows.length === 0) {
    log('\nNo rows to insert. Exiting.');
    return;
  }

  log(`\nInserting ${rows.length.toLocaleString()} rows...`);
  await d1Etl.batchFile(buildMultiRowInserts(insertPrefix, rows));

  // Step 6: Verify final count
  log('Verifying final row count in cross_references...');
  const countResult = await d1.query(
    `SELECT COUNT(*) AS cnt FROM cross_references WHERE source = ?`,
    [SOURCE],
  );
  const finalCount = countResult.results[0]?.['cnt'] as number ?? 0;

  console.log('\n=======================================');
  log('ETL complete.');
  log(`  Final cross_references row count (source='${SOURCE}'): ${finalCount.toLocaleString()}`);
  log('Done.');
}

main().catch((err) => {
  console.error('[etl:crossrefs] Unexpected error:', err);
  process.exit(1);
});
