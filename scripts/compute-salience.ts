#!/usr/bin/env tsx
/**
 * compute-salience.ts
 *
 * Computes per-book per-topic salience weights and inserts them into the
 * nave_topic_book_salience D1 table.
 *
 * Salience formula:
 *   salience = (verse_count / total_book_verses) * chapter_coverage * log2(1 + verse_count)
 *   where chapter_coverage = distinct_chapters / total_book_chapters
 *
 * This pre-computation enables witness ranking to use density-based scoring
 * instead of raw verse counts, surfacing books that concentrate their
 * coverage on a topic rather than merely mentioning it many times.
 *
 * Usage:
 *   npx tsx scripts/compute-salience.ts
 *   npm run etl:salience
 *
 * Required environment variables:
 *   CLOUDFLARE_API_TOKEN
 *   CLOUDFLARE_ACCOUNT_ID
 *   D1_DATABASE_ID
 */

import './load-env.js';
import { d1 } from '../src/lib/cloudflare.js';
import { d1Etl, buildMultiRowInserts } from '../src/lib/cloudflare-etl.js';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/** Total chapter counts per book (66 canonical books, book_id 1–66). */
const BOOK_TOTAL_CHAPTERS: Record<number, number> = {
  1: 50,  // Genesis
  2: 40,  // Exodus
  3: 27,  // Leviticus
  4: 36,  // Numbers
  5: 34,  // Deuteronomy
  6: 24,  // Joshua
  7: 21,  // Judges
  8: 4,   // Ruth
  9: 31,  // 1 Samuel
  10: 24, // 2 Samuel
  11: 22, // 1 Kings
  12: 25, // 2 Kings
  13: 29, // 1 Chronicles
  14: 36, // 2 Chronicles
  15: 10, // Ezra
  16: 13, // Nehemiah
  17: 10, // Esther
  18: 42, // Job
  19: 150, // Psalms
  20: 31, // Proverbs
  21: 12, // Ecclesiastes
  22: 8,  // Song of Solomon
  23: 66, // Isaiah
  24: 52, // Jeremiah
  25: 5,  // Lamentations
  26: 48, // Ezekiel
  27: 12, // Daniel
  28: 14, // Hosea
  29: 3,  // Joel
  30: 9,  // Amos
  31: 1,  // Obadiah
  32: 4,  // Jonah
  33: 7,  // Micah
  34: 3,  // Nahum
  35: 3,  // Habakkuk
  36: 3,  // Zephaniah
  37: 2,  // Haggai
  38: 14, // Zechariah
  39: 4,  // Malachi
  40: 28, // Matthew
  41: 16, // Mark
  42: 24, // Luke
  43: 21, // John
  44: 28, // Acts
  45: 16, // Romans
  46: 16, // 1 Corinthians
  47: 13, // 2 Corinthians
  48: 6,  // Galatians
  49: 6,  // Ephesians
  50: 4,  // Philippians
  51: 4,  // Colossians
  52: 5,  // 1 Thessalonians
  53: 3,  // 2 Thessalonians
  54: 6,  // 1 Timothy
  55: 4,  // 2 Timothy
  56: 3,  // Titus
  57: 1,  // Philemon
  58: 13, // Hebrews
  59: 5,  // James
  60: 5,  // 1 Peter
  61: 3,  // 2 Peter
  62: 5,  // 1 John
  63: 1,  // 2 John
  64: 1,  // 3 John
  65: 1,  // Jude
  66: 22, // Revelation
};

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

function log(msg: string): void {
  console.log(`[etl:salience] ${msg}`);
}

// ---------------------------------------------------------------------------
// Data fetching
// ---------------------------------------------------------------------------

interface BookVerseCounts {
  bookId: number;
  totalVerses: number;
}

/**
 * Fetch total verse count per book from KJV translation (translation_id = 1).
 */
async function fetchBookVerseCounts(): Promise<Map<number, number>> {
  log('Fetching total verse counts per book (KJV)...');

  const result = await d1.query(
    'SELECT book_id, COUNT(*) AS total_verses FROM verses WHERE translation_id = 1 GROUP BY book_id ORDER BY book_id',
    [],
  );

  const counts = new Map<number, number>();
  for (const row of result.results) {
    const r = row as unknown as BookVerseCounts;
    counts.set(r.bookId ?? (row as Record<string, number>)['book_id'], Number((row as Record<string, number>)['total_verses']));
  }

  log(`  Loaded verse counts for ${counts.size} books`);
  return counts;
}

interface TopicBookStat {
  topic_id: number;
  book_id: number;
  verse_count: number;
  chapter_count: number;
}

/**
 * Fetch per-topic per-book stats from nave_topic_verses.
 * Returns verse_count and distinct chapter_count for each (topic_id, book_id) pair.
 *
 * Fetches in pages to avoid hitting D1 REST response size limits.
 */
async function fetchTopicBookStats(): Promise<TopicBookStat[]> {
  log('Fetching per-topic per-book stats from nave_topic_verses...');

  const stats: TopicBookStat[] = [];
  const pageSize = 10000;
  let offset = 0;

  while (true) {
    const result = await d1.query(
      `SELECT topic_id, book_id,
              COUNT(*) AS verse_count,
              COUNT(DISTINCT chapter) AS chapter_count
       FROM nave_topic_verses
       GROUP BY topic_id, book_id
       ORDER BY topic_id, book_id
       LIMIT ? OFFSET ?`,
      [pageSize, offset],
    );

    for (const row of result.results) {
      const r = row as unknown as TopicBookStat;
      stats.push({
        topic_id: Number((row as Record<string, unknown>)['topic_id']),
        book_id: Number((row as Record<string, unknown>)['book_id']),
        verse_count: Number((row as Record<string, unknown>)['verse_count']),
        chapter_count: Number((row as Record<string, unknown>)['chapter_count']),
      });
    }

    if (result.results.length < pageSize) break;
    offset += pageSize;
  }

  log(`  Loaded ${stats.length.toLocaleString()} topic-book stat rows`);
  return stats;
}

// ---------------------------------------------------------------------------
// Salience computation
// ---------------------------------------------------------------------------

interface SalienceRow {
  topicId: number;
  bookId: number;
  salience: number;
  verseCount: number;
  chapterCount: number;
}

/**
 * Compute salience for all (topic_id, book_id) pairs.
 *
 * Formula:
 *   chapter_coverage = distinct_chapters / total_book_chapters
 *   salience = (verse_count / total_book_verses) * chapter_coverage * log2(1 + verse_count)
 */
function computeSalience(
  stats: TopicBookStat[],
  bookVerseCounts: Map<number, number>,
): SalienceRow[] {
  const rows: SalienceRow[] = [];

  for (const stat of stats) {
    const totalBookVerses = bookVerseCounts.get(stat.book_id);
    if (!totalBookVerses || totalBookVerses === 0) continue;

    const totalBookChapters = BOOK_TOTAL_CHAPTERS[stat.book_id];
    if (!totalBookChapters || totalBookChapters === 0) continue;

    const chapterCoverage = stat.chapter_count / totalBookChapters;
    const salience =
      (stat.verse_count / totalBookVerses) *
      chapterCoverage *
      Math.log2(1 + stat.verse_count);

    rows.push({
      topicId: stat.topic_id,
      bookId: stat.book_id,
      salience,
      verseCount: stat.verse_count,
      chapterCount: stat.chapter_count,
    });
  }

  return rows;
}

// ---------------------------------------------------------------------------
// D1 insertion
// ---------------------------------------------------------------------------

/**
 * Clear existing salience data and insert fresh computed rows.
 *
 * Uses DELETE + INSERT OR REPLACE strategy: clears the table first so
 * subsequent runs are idempotent without relying on UPDATE semantics.
 */
async function insertSalienceRows(rows: SalienceRow[]): Promise<void> {
  log('Clearing existing nave_topic_book_salience data...');
  await d1Etl.batchFile('DELETE FROM nave_topic_book_salience;\n');
  log('  Cleared');

  if (rows.length === 0) {
    log('  No salience rows to insert');
    return;
  }

  log(`Inserting ${rows.length.toLocaleString()} salience rows...`);

  const insertRows = rows.map((r) => [
    r.topicId,
    r.bookId,
    r.salience,
    r.verseCount,
    r.chapterCount,
  ]);

  const sql = buildMultiRowInserts(
    'INSERT OR REPLACE INTO nave_topic_book_salience (topic_id, book_id, salience, verse_count, chapter_count) VALUES',
    insertRows,
  );

  await d1Etl.batchFile(sql);
  log(`  Inserted ${rows.length.toLocaleString()} rows`);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log('Bible Study MCP Server — Topic Book Salience ETL');
  console.log('==========================================\n');

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

  // Step 1: Fetch book verse counts (KJV as canonical count)
  const bookVerseCounts = await fetchBookVerseCounts();

  // Step 2: Fetch per-topic per-book stats from nave_topic_verses
  const stats = await fetchTopicBookStats();

  // Step 3: Compute salience weights
  log('Computing salience weights...');
  const salienceRows = computeSalience(stats, bookVerseCounts);
  const uniqueTopics = new Set(salienceRows.map((r) => r.topicId)).size;
  log(
    `  Computed ${salienceRows.length.toLocaleString()} salience rows across ${uniqueTopics.toLocaleString()} topics`,
  );

  // Step 4: Insert into D1
  await insertSalienceRows(salienceRows);

  console.log('\n===========================================');
  log(
    `ETL complete. ${salienceRows.length.toLocaleString()} rows inserted for ${uniqueTopics.toLocaleString()} topics.`,
  );
}

main().catch((err) => {
  console.error('[etl:salience] Unexpected error:', err);
  process.exit(1);
});
