// Tool: topical_search
//
// Combines Nave's Topical Bible (curated editorial index) with Vectorize
// semantic search to provide comprehensive topic-based Bible discovery.
//
// Flow:
//   1. Concurrently: D1 Nave's query + Vectorize embedding query + topic expansion
//   2. Fetch verse text for each result set from D1
//   3. Deduplicate — Nave's entries take precedence (they carry editorial notes)
//   4. Build major witnesses from expanded topics + semantic results
//   5. Return combined results with source attribution and match explanations

import type { ToolHandler } from '@mctx-ai/mcp-server';
import { T } from '@mctx-ai/mcp-server';
import { d1, vectorize, vectorizeTopics, workersAi } from '../lib/cloudflare.js';
import type { Citation } from '../lib/bible-utils.js';
import { getTranslation, ensureInitialized } from '../lib/bible-utils.js';

// ─── Types ───────────────────────────────────────────────────────────────────

interface TopicalResult {
  text: string;
  citation: Citation;
  source: 'naves' | 'semantic' | 'both';
  note?: string;
  match_reason?: string;
}

interface MajorWitness {
  book: string;
  testament: string;
  verse_count: number;
  chapter_count: number;
  matched_topics: string[];
  narrative?: string;
  match_reason: string;
  representative_verse: {
    text: string;
    citation: Citation;
  };
  why_this_book_matters?: string;
  themes_matched?: string[];
  suggested_anchor_passages?: string[];
  narrative_reason?: string;
}

interface TopicalSearchResponse {
  topic: string;
  results: TopicalResult[];
  total_results: number;
  major_witnesses: MajorWitness[];
}

// ─── Constants ────────────────────────────────────────────────────────────────

// Overfetch from Vectorize to account for deduplication across translations.
const VECTORIZE_OVERFETCH_MULTIPLIER = 8;

const MAJOR_WITNESS_MIN_VERSES = 5;
const MAJOR_WITNESS_MIN_CHAPTERS = 2;
const MAX_MAJOR_WITNESSES = 12;
// Witnesses scoring below this fraction of the top witness score are excluded,
// even if fewer than MAX_MAJOR_WITNESSES have been emitted.
const WITNESS_SCORE_CUTOFF = 0.55;
const MAX_EXPANDED_TOPICS = 50;
const WITNESS_INTERLEAVE_INTERVAL = 5;

// ─── D1 row shapes ────────────────────────────────────────────────────────────

interface NaveVerseRow {
  book_name: string;
  chapter: number;
  verse: number;
  translation_abbrev: string;
  text: string;
  note: string | null;
  topic_name: string;
}

interface VerseRow {
  book_id: number;
  chapter: number;
  verse: number;
  book_name: string;
  translation_abbrev: string;
  text: string;
}

interface WitnessCandidate {
  book_id: number;
  book_name: string;
  testament: string;
  verse_count: number;
  chapter_count: number;
  min_chapter: number;
  max_chapter: number;
  topic_names: string;
}

// ─── Total chapter counts for all 66 canonical books ─────────────────────────

const BOOK_TOTAL_CHAPTERS: Record<string, number> = {
  Genesis: 50,
  Exodus: 40,
  Leviticus: 27,
  Numbers: 36,
  Deuteronomy: 34,
  Joshua: 24,
  Judges: 21,
  Ruth: 4,
  '1 Samuel': 31,
  '2 Samuel': 24,
  '1 Kings': 22,
  '2 Kings': 25,
  '1 Chronicles': 29,
  '2 Chronicles': 36,
  Ezra: 10,
  Nehemiah: 13,
  Esther: 10,
  Job: 42,
  Psalms: 150,
  Proverbs: 31,
  Ecclesiastes: 12,
  'Song of Solomon': 8,
  Isaiah: 66,
  Jeremiah: 52,
  Lamentations: 5,
  Ezekiel: 48,
  Daniel: 12,
  Hosea: 14,
  Joel: 3,
  Amos: 9,
  Obadiah: 1,
  Jonah: 4,
  Micah: 7,
  Nahum: 3,
  Habakkuk: 3,
  Zephaniah: 3,
  Haggai: 2,
  Zechariah: 14,
  Malachi: 4,
  Matthew: 28,
  Mark: 16,
  Luke: 24,
  John: 21,
  Acts: 28,
  Romans: 16,
  '1 Corinthians': 16,
  '2 Corinthians': 13,
  Galatians: 6,
  Ephesians: 6,
  Philippians: 4,
  Colossians: 4,
  '1 Thessalonians': 5,
  '2 Thessalonians': 3,
  '1 Timothy': 6,
  '2 Timothy': 4,
  Titus: 3,
  Philemon: 1,
  Hebrews: 13,
  James: 5,
  '1 Peter': 5,
  '2 Peter': 3,
  '1 John': 5,
  '2 John': 1,
  '3 John': 1,
  Jude: 1,
  Revelation: 22,
};

// ─── Nave's topic relevance scoring ───────────────────────────────────────────

// Returns a [0, 1] relevance score for a Nave's topic name against the query.
// Exact match → 1.0; topic name starts with query → 0.8; query is a word in
// topic name → 0.6; topic name contains query → 0.4; partial substring → 0.2.
// This gives a simple but meaningful ordering within the Nave's-only pool.
function naveTopicRelevance(topicName: string, query: string): number {
  const t = topicName.toLowerCase();
  const q = query.toLowerCase();
  if (t === q) return 1.0;
  if (t.startsWith(q)) return 0.8;
  const words = t.split(/\s+/);
  if (words.some((w) => w === q)) return 0.6;
  if (t.includes(q)) return 0.4;
  return 0.2;
}

// ─── Semantic topic + book search (single Vectorize topics index query) ──────

// Queries the Vectorize topics index once and splits results by ID prefix into
// topic-level and book-level matches. Falls back to empty arrays if the topic
// index is not configured.
async function searchSemanticTopicsAndBooks(
  queryVector: number[],
): Promise<{
  topics: Array<{ id: number; name: string; score: number }>;
  books: Array<{ book_id: number; score: number }>;
}> {
  const matches = await vectorizeTopics.query(queryVector, { topK: 40 });
  if (matches.length === 0) return { topics: [], books: [] };

  const topics: Array<{ id: number; name: string; score: number }> = [];
  const books: Array<{ book_id: number; score: number }> = [];

  for (const match of matches) {
    const meta = match.metadata;
    if (!meta) continue;

    if (match.id.startsWith('topic-')) {
      const topicId = meta['topic_id'] as number | undefined;
      const topicName = meta['name'] as string | undefined;
      if (!topicId || !topicName) continue;
      topics.push({ id: topicId, name: topicName, score: match.score });
    } else if (match.id.startsWith('book-')) {
      const bookId = meta['book_id'] as number | undefined;
      if (!bookId) continue;
      books.push({ book_id: bookId, score: match.score });
    }
  }

  console.error(`[DEBUG] vectorize topic matches: ${topics.length} topics, ${books.length} books out of ${matches.length} total`);
  return { topics, books };
}

// ─── Salience fetch ───────────────────────────────────────────────────────────

// Queries nave_topic_book_salience for matching topic+book combinations.
// Returns a Map keyed by 'bookId:topicId' → salience score.
async function fetchSalience(
  topicIds: number[],
  bookIds: number[],
): Promise<Map<string, number>> {
  if (topicIds.length === 0 || bookIds.length === 0) return new Map();

  const topicPlaceholders = topicIds.map(() => '?').join(', ');
  const bookPlaceholders = bookIds.map(() => '?').join(', ');

  const result = await d1.query(
    `SELECT topic_id, book_id, salience
     FROM nave_topic_book_salience
     WHERE topic_id IN (${topicPlaceholders})
       AND book_id IN (${bookPlaceholders})`,
    [...topicIds, ...bookIds],
  );

  const salienceMap = new Map<string, number>();
  for (const row of result.results) {
    const topicId = row['topic_id'] as number;
    const bookId = row['book_id'] as number;
    const salience = row['salience'] as number;
    salienceMap.set(`${bookId}:${topicId}`, salience);
  }

  return salienceMap;
}

// ─── Nave's expanded topic query (lightweight LIKE fallback) ──────────────────

async function queryExpandedTopicsByLike(
  topic: string,
): Promise<Array<{ id: number; name: string }>> {
  // Split compound phrases into individual content words so "God's
  // faithfulness during suffering" matches topics containing any of those
  // words. Skip short/common words to avoid overly broad matches.
  const stopWords = new Set([
    'the', 'and', 'for', 'but', 'not', 'with', 'from', 'that', 'this',
    'into', 'over', 'upon', 'also', 'than', 'then', 'them', 'they',
    'have', 'been', 'were', 'will', 'does', 'done', 'during', 'about',
    "god's", 'gods',
  ]);

  const words = topic
    .toLowerCase()
    .split(/\s+/)
    .filter((w) => w.length >= 4 && !stopWords.has(w))
    .map((w) => w.replace(/%/g, '\\%').replace(/_/g, '\\_'));

  if (words.length === 0) {
    // Fall back to full-phrase match for single-word or very short queries.
    const escaped = topic.toLowerCase().replace(/%/g, '\\%').replace(/_/g, '\\_');
    const result = await d1.query(
      `SELECT DISTINCT nt.id, nt.topic_name
       FROM nave_topics nt
       WHERE nt.normalized_topic LIKE ? ESCAPE '\\'
       LIMIT ${MAX_EXPANDED_TOPICS}`,
      [`%${escaped}%`],
    );
    return result.results.map((row) => ({
      id: row['id'] as number,
      name: row['topic_name'] as string,
    }));
  }

  const whereClauses = words.map(() => `nt.normalized_topic LIKE ? ESCAPE '\\'`);
  const params = words.map((w) => `%${w}%`);

  const result = await d1.query(
    `SELECT DISTINCT nt.id, nt.topic_name
     FROM nave_topics nt
     WHERE ${whereClauses.join(' OR ')}
     LIMIT ${MAX_EXPANDED_TOPICS}`,
    params,
  );

  return result.results.map((row) => ({
    id: row['id'] as number,
    name: row['topic_name'] as string,
  }));
}

// ─── Salience lookup for co-occurring topics ────────────────────────────────

// Returns all salience entries for a set of topic IDs (no book filter).
// Used to identify which co-occurring topics have high salience for specific books.
async function fetchCooccurringSalience(
  topicIds: number[],
): Promise<Array<{ topic_id: number; book_id: number; salience: number }>> {
  if (topicIds.length === 0) return [];

  const placeholders = topicIds.map(() => '?').join(', ');
  const result = await d1.query(
    `SELECT topic_id, book_id, salience
     FROM nave_topic_book_salience
     WHERE topic_id IN (${placeholders})
       AND salience >= 0.5
     ORDER BY salience DESC
     LIMIT 200`,
    topicIds,
  );

  return result.results.map((row) => ({
    topic_id: row['topic_id'] as number,
    book_id: row['book_id'] as number,
    salience: row['salience'] as number,
  }));
}

// ─── Topic co-occurrence expansion ───────────────────────────────────────────

// Given a set of seed topic IDs, finds other topics that share the most verses
// with them. This discovers related topics like AFFLICTIONS for SUFFERING queries
// that neither LIKE nor Vectorize can surface directly.
const MAX_COOCCURRENCE_TOPICS = 20;

async function expandTopicsByCooccurrence(
  seedTopicIds: number[],
): Promise<Array<{ id: number; name: string; shared_verses: number }>> {
  if (seedTopicIds.length === 0) return [];

  const placeholders = seedTopicIds.map(() => '?').join(', ');

  // Find topics that share the most verses with our seed topics.
  // Filter out overly broad topics (> 2000 total verses) like JESUS or GOD.
  const result = await d1.query(
    `SELECT
       nt.id,
       nt.topic_name,
       COUNT(*) AS shared_verses,
       (SELECT COUNT(*) FROM nave_topic_verses ntv3 WHERE ntv3.topic_id = ntv2.topic_id) AS total_verses
     FROM nave_topic_verses ntv1
     JOIN nave_topic_verses ntv2
       ON ntv2.book_id = ntv1.book_id
      AND ntv2.chapter = ntv1.chapter
      AND ntv2.verse = ntv1.verse
     JOIN nave_topics nt ON nt.id = ntv2.topic_id
     WHERE ntv1.topic_id IN (${placeholders})
       AND ntv2.topic_id NOT IN (${placeholders})
     GROUP BY ntv2.topic_id
     HAVING shared_verses >= 3
       AND total_verses <= 2000
     ORDER BY shared_verses DESC
     LIMIT ${MAX_COOCCURRENCE_TOPICS}`,
    [...seedTopicIds, ...seedTopicIds],
  );

  return result.results.map((row) => ({
    id: row['id'] as number,
    name: row['topic_name'] as string,
    shared_verses: row['shared_verses'] as number,
  }));
}

// ─── Witness aggregation ──────────────────────────────────────────────────────

const WITNESS_CHUNK_SIZE = 100;

async function aggregateWitnesses(
  topicIds: number[],
): Promise<WitnessCandidate[]> {
  if (topicIds.length === 0) return [];

  // Defensively chunk if topic IDs exceed 100 to stay within D1 parameter limits.
  const chunks: number[][] = [];
  for (let i = 0; i < topicIds.length; i += WITNESS_CHUNK_SIZE) {
    chunks.push(topicIds.slice(i, i + WITNESS_CHUNK_SIZE));
  }

  // Run all chunks and merge results, aggregating by book_id.
  const bookAgg = new Map<
    number,
    {
      book_id: number;
      book_name: string;
      testament: string;
      verse_count: number;
      chapter_count_sum: number;
      min_chapter: number;
      max_chapter: number;
      topic_name_set: Set<string>;
    }
  >();

  await Promise.all(
    chunks.map(async (chunk) => {
      const placeholders = chunk.map(() => '?').join(', ');
      const result = await d1.query(
        `SELECT
           ntv.book_id,
           b.name AS book_name,
           b.testament,
           COUNT(*) AS verse_count,
           COUNT(DISTINCT ntv.chapter) AS chapter_count,
           MIN(ntv.chapter) AS min_chapter,
           MAX(ntv.chapter) AS max_chapter,
           GROUP_CONCAT(DISTINCT nt.topic_name) AS topic_names
         FROM nave_topic_verses ntv
         JOIN nave_topics nt ON nt.id = ntv.topic_id
         JOIN books b ON b.id = ntv.book_id
         WHERE ntv.topic_id IN (${placeholders})
         GROUP BY ntv.book_id
         ORDER BY verse_count DESC
         LIMIT 30`,
        chunk,
      );

      for (const row of result.results) {
        const bookId = row['book_id'] as number;
        const bookName = row['book_name'] as string;
        const testament = row['testament'] as string;
        const verseCount = row['verse_count'] as number;
        const minChapter = row['min_chapter'] as number;
        const maxChapter = row['max_chapter'] as number;
        const topicNamesStr = row['topic_names'] as string;

        const topicNames = topicNamesStr
          ? topicNamesStr.split(',').map((n) => n.trim())
          : [];

        if (!bookAgg.has(bookId)) {
          bookAgg.set(bookId, {
            book_id: bookId,
            book_name: bookName,
            testament,
            verse_count: 0,
            chapter_count_sum: 0,
            min_chapter: minChapter,
            max_chapter: maxChapter,
            topic_name_set: new Set(),
          });
        }

        const agg = bookAgg.get(bookId)!;
        agg.verse_count += verseCount;
        agg.min_chapter = Math.min(agg.min_chapter, minChapter);
        agg.max_chapter = Math.max(agg.max_chapter, maxChapter);

        // Use the actual distinct chapter count from SQL rather than
        // approximating from min/max range (which inflates the count for
        // books with sparse topic coverage like Psalms).
        const chapterCount = row['chapter_count'] as number;
        agg.chapter_count_sum += chapterCount;

        for (const name of topicNames) {
          if (name) agg.topic_name_set.add(name);
        }
      }
    }),
  );

  // Convert aggregated map to WitnessCandidate array sorted by verse_count desc.
  const candidates: WitnessCandidate[] = Array.from(bookAgg.values())
    .map((agg) => ({
      book_id: agg.book_id,
      book_name: agg.book_name,
      testament: agg.testament,
      verse_count: agg.verse_count,
      chapter_count: agg.chapter_count_sum,
      min_chapter: agg.min_chapter,
      max_chapter: agg.max_chapter,
      topic_names: Array.from(agg.topic_name_set).join(','),
    }))
    .sort((a, b) => b.verse_count - a.verse_count);

  return candidates;
}

// ─── Narrative detection ──────────────────────────────────────────────────────

function detectNarrative(
  candidate: WitnessCandidate,
  matchedTopics: string[],
): string | undefined {
  const totalChapters = BOOK_TOTAL_CHAPTERS[candidate.book_name];

  for (const topic of matchedTopics) {
    const words = topic.trim().split(/\s+/);
    // A narrative topic has 1-3 words (e.g., JOSEPH, MOSES, KING DAVID).
    if (words.length < 1 || words.length > 3) continue;

    // If the candidate spans fewer chapters than the full book, it's a subset.
    const spannedChapters = candidate.max_chapter - candidate.min_chapter + 1;
    const isSubset =
      totalChapters !== undefined && spannedChapters < totalChapters * 0.8;

    if (isSubset) {
      // Title-case the topic name as the narrative label.
      return topic
        .split(' ')
        .map((w) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
        .join(' ');
    }
  }

  return undefined;
}

// ─── Match reason generation ──────────────────────────────────────────────────

function buildWitnessMatchReason(
  candidate: WitnessCandidate,
  narrative?: string,
): string {
  const topicList = candidate.topic_names
    .split(',')
    .slice(0, 3)
    .join(', ');

  if (narrative) {
    return `${narrative} narrative: ${candidate.verse_count} topical references across ${candidate.book_name} ${candidate.min_chapter}-${candidate.max_chapter} (${topicList})`;
  }
  return `${candidate.verse_count} topical references across ${candidate.chapter_count} chapters (${topicList})`;
}

// ─── Representative verse selection ──────────────────────────────────────────

async function fetchRepresentativeVerse(
  candidate: WitnessCandidate,
  semanticCoords: Map<
    string,
    {
      book_id: number;
      chapter: number;
      verse: number;
      translation_id: number;
      score: number;
    }
  >,
  semanticVerseMap: Map<string, VerseRow>,
): Promise<{ text: string; citation: Citation }> {
  // Try to find the highest-scoring Vectorize hit for this book.
  let bestScore = -Infinity;
  let bestVerseRow: VerseRow | undefined;

  for (const [coordKey, coord] of semanticCoords) {
    if (coord.book_id !== candidate.book_id) continue;
    const verseRow = semanticVerseMap.get(coordKey);
    if (!verseRow) continue;
    if (coord.score > bestScore) {
      bestScore = coord.score;
      bestVerseRow = verseRow;
    }
  }

  if (bestVerseRow) {
    return {
      text: bestVerseRow.text,
      citation: {
        book: bestVerseRow.book_name,
        chapter: bestVerseRow.chapter,
        verse: bestVerseRow.verse,
        translation: bestVerseRow.translation_abbrev,
      },
    };
  }

  // Fallback: fetch verse 1 of the most topic-dense chapter in this book.
  const kjvTranslation = getTranslation('KJV');
  const translationFilter = kjvTranslation
    ? `AND v.translation_id = ?`
    : `AND t.abbreviation = 'KJV'`;
  const translationParams: unknown[] = kjvTranslation ? [kjvTranslation.id] : [];

  const fallbackResult = await d1.query(
    `SELECT
       ntv.chapter,
       COUNT(*) AS topic_hits
     FROM nave_topic_verses ntv
     WHERE ntv.book_id = ?
     GROUP BY ntv.chapter
     ORDER BY topic_hits DESC
     LIMIT 1`,
    [candidate.book_id],
  );

  const denseChapter =
    fallbackResult.results.length > 0
      ? (fallbackResult.results[0]['chapter'] as number)
      : 1;

  const verseResult = await d1.query(
    `SELECT
       v.book_id,
       v.chapter,
       v.verse,
       b.name AS book_name,
       t.abbreviation AS translation_abbrev,
       v.text
     FROM verses v
     JOIN books b ON b.id = v.book_id
     JOIN translations t ON t.id = v.translation_id
     WHERE v.book_id = ?
       AND v.chapter = ?
       AND v.verse = 1
       ${translationFilter}
     LIMIT 1`,
    [candidate.book_id, denseChapter, ...translationParams],
  );

  if (verseResult.results.length > 0) {
    const r = verseResult.results[0] as unknown as VerseRow;
    return {
      text: r.text,
      citation: {
        book: r.book_name,
        chapter: r.chapter,
        verse: r.verse,
        translation: r.translation_abbrev,
      },
    };
  }

  // Last resort: return empty placeholder (should never happen in practice).
  return {
    text: '',
    citation: {
      book: candidate.book_name,
      chapter: candidate.min_chapter,
      verse: 1,
      translation: 'KJV',
    },
  };
}

// ─── Enrichment helpers ───────────────────────────────────────────────────────

// Builds a human-readable sentence explaining why a book is a principal witness.
// Uses pre-computed salience data to find the 2-3 most central topics for this
// book, then synthesizes a sentence. Falls back to topic_names when salience
// data is unavailable.
function buildWhyThisBookMatters(
  candidate: WitnessCandidate,
  salienceMap: Map<string, number>,
  salienceTopicIds: number[],
  expandedTopics: Array<{ id: number; name: string }>,
): string {
  // Collect per-topic salience scores for this book.
  const topicById = new Map(expandedTopics.map((t) => [t.id, t.name]));
  const scored: Array<{ name: string; salience: number }> = [];

  for (const topicId of salienceTopicIds) {
    const salience = salienceMap.get(`${candidate.book_id}:${topicId}`) ?? 0;
    const name = topicById.get(topicId);
    if (name && salience > 0) {
      scored.push({ name, salience });
    }
  }

  scored.sort((a, b) => b.salience - a.salience);
  const top = scored.slice(0, 3);

  if (top.length > 0) {
    const topicList = top.map((t) => t.name).join(', ');
    const totalChapters = BOOK_TOTAL_CHAPTERS[candidate.book_name] ?? candidate.chapter_count;
    const coveragePct = Math.round((candidate.chapter_count / totalChapters) * 100);
    return `${candidate.book_name} concentrates on ${topicList}, with ${candidate.verse_count} topical references spanning ${coveragePct}% of the book.`;
  }

  // Fallback: use first 2-3 topic names from the candidate directly.
  const fallbackTopics = candidate.topic_names
    .split(',')
    .map((n) => n.trim())
    .filter(Boolean)
    .slice(0, 3)
    .join(', ');

  return fallbackTopics
    ? `${candidate.book_name} addresses ${fallbackTopics} with ${candidate.verse_count} topical references across ${candidate.chapter_count} chapters.`
    : `${candidate.book_name} is a principal witness with ${candidate.verse_count} topical references.`;
}

// Builds the list of query-relevant themes for a witness book.
// Filters topic_names to only those topics that appear in expandedTopics
// (the query-matched topic set), then sorts by salience descending.
function buildThemesMatched(
  candidate: WitnessCandidate,
  expandedTopics: Array<{ id: number; name: string }>,
  salienceMap: Map<string, number>,
  salienceTopicIds: number[],
): string[] {
  const expandedNameSet = new Set(expandedTopics.map((t) => t.name));

  const candidateTopics = candidate.topic_names
    .split(',')
    .map((n) => n.trim())
    .filter(Boolean);

  // Keep only topics that are in the query-matched expanded topic set.
  const matched = candidateTopics.filter((name) => expandedNameSet.has(name));

  if (matched.length === 0) {
    // Fallback: return first 5 topics from candidate.
    return candidateTopics.slice(0, 5);
  }

  // Build a salience score lookup by topic name for this book.
  const topicIdByName = new Map(expandedTopics.map((t) => [t.name, t.id]));
  const getSalience = (name: string): number => {
    const topicId = topicIdByName.get(name);
    if (!topicId || !salienceTopicIds.includes(topicId)) return 0;
    return salienceMap.get(`${candidate.book_id}:${topicId}`) ?? 0;
  };

  matched.sort((a, b) => getSalience(b) - getSalience(a));
  return matched;
}

// Row shape returned by the anchor passage batch query.
interface AnchorChapterRow {
  book_id: number;
  chapter: number;
  min_verse: number;
  max_verse: number;
  hit_count: number;
}

// Issues a single batch D1 query to retrieve chapter-level verse density for
// all witness books × matched topics. Returns a map from book_id to sorted
// chapter rows (ascending chapter order).
async function fetchAnchorPassageData(
  expandedTopicIds: number[],
  candidateBookIds: number[],
): Promise<Map<number, AnchorChapterRow[]>> {
  if (expandedTopicIds.length === 0 || candidateBookIds.length === 0) {
    return new Map();
  }

  const topicPlaceholders = expandedTopicIds.map(() => '?').join(', ');
  const bookPlaceholders = candidateBookIds.map(() => '?').join(', ');

  const result = await d1.query(
    `SELECT book_id, chapter, MIN(verse) AS min_verse, MAX(verse) AS max_verse, COUNT(*) AS hit_count
     FROM nave_topic_verses
     WHERE topic_id IN (${topicPlaceholders})
       AND book_id IN (${bookPlaceholders})
     GROUP BY book_id, chapter
     ORDER BY book_id, chapter`,
    [...expandedTopicIds, ...candidateBookIds],
  );

  const byBook = new Map<number, AnchorChapterRow[]>();
  for (const row of result.results) {
    const bookId = row['book_id'] as number;
    if (!byBook.has(bookId)) byBook.set(bookId, []);
    byBook.get(bookId)!.push({
      book_id: bookId,
      chapter: row['chapter'] as number,
      min_verse: row['min_verse'] as number,
      max_verse: row['max_verse'] as number,
      hit_count: row['hit_count'] as number,
    });
  }

  return byBook;
}

// Clusters chapter rows for a single book into passage range strings.
// Consecutive chapters (gap <= 2) are merged into spans. Short books whose
// coverage >= 80% of total chapters collapse to just the book name.
// Returns at most 3 passage ranges.
function clusterToPassageRanges(
  rows: AnchorChapterRow[],
  bookName: string,
  totalBookChapters: number,
): string[] {
  if (rows.length === 0) return [];

  // Short book: if coverage spans >= 80% of the book, just return the book name.
  const minChapter = Math.min(...rows.map((r) => r.chapter));
  const maxChapter = Math.max(...rows.map((r) => r.chapter));
  const spanCount = maxChapter - minChapter + 1;
  if (spanCount >= totalBookChapters * 0.8 && totalBookChapters <= 5) {
    return [bookName];
  }

  // Filter chapters with at least 2 hits to avoid sparse anchors (unless the
  // entire book is sparse, in which case keep the 3 highest-hit chapters).
  const MIN_HIT_THRESHOLD = 2;
  let qualifying = rows.filter((r) => r.hit_count >= MIN_HIT_THRESHOLD);
  if (qualifying.length === 0) {
    // Sparse coverage: take the top 3 chapters by hit count.
    qualifying = [...rows].sort((a, b) => b.hit_count - a.hit_count).slice(0, 3);
  }

  // Sort qualifying chapters by hit count descending to pick the densest first,
  // then re-sort by chapter number before clustering.
  qualifying.sort((a, b) => b.hit_count - a.hit_count);
  // Take the top chapters (cap to avoid too many small ranges).
  const topChapters = qualifying.slice(0, 12);
  topChapters.sort((a, b) => a.chapter - b.chapter);

  // Cluster consecutive chapters (gap <= 2) into spans.
  const spans: Array<{ start: number; end: number; totalHits: number; minVerse: number; maxVerse: number }> = [];
  let currentSpan: typeof spans[0] | null = null;

  for (const row of topChapters) {
    if (!currentSpan) {
      currentSpan = { start: row.chapter, end: row.chapter, totalHits: row.hit_count, minVerse: row.min_verse, maxVerse: row.max_verse };
    } else if (row.chapter - currentSpan.end <= 2) {
      currentSpan.end = row.chapter;
      currentSpan.totalHits += row.hit_count;
      currentSpan.minVerse = Math.min(currentSpan.minVerse, row.min_verse);
      currentSpan.maxVerse = Math.max(currentSpan.maxVerse, row.max_verse);
    } else {
      spans.push(currentSpan);
      currentSpan = { start: row.chapter, end: row.chapter, totalHits: row.hit_count, minVerse: row.min_verse, maxVerse: row.max_verse };
    }
  }
  if (currentSpan) spans.push(currentSpan);

  // Sort spans by aggregate hit count descending.
  spans.sort((a, b) => b.totalHits - a.totalHits);

  // Emit up to 3 passage range strings.
  return spans.slice(0, 3).map((span) => {
    if (span.start === span.end) {
      // Single chapter: include verse range only if it's a narrow focus.
      const verseRange = span.maxVerse - span.minVerse;
      if (verseRange < 10 && span.minVerse !== span.maxVerse) {
        return `${bookName} ${span.start}:${span.minVerse}-${span.maxVerse}`;
      }
      return `${bookName} ${span.start}`;
    }
    return `${bookName} ${span.start}-${span.end}`;
  });
}

// Builds the narrative_reason string when detectNarrative returns a result.
// Returns undefined for non-narrative books.
function buildNarrativeReason(
  narrative: string | undefined,
  candidate: WitnessCandidate,
): string | undefined {
  if (!narrative) return undefined;
  return `The ${narrative} (${candidate.book_name} ${candidate.min_chapter}-${candidate.max_chapter}) addresses this theme through its narrative arc rather than systematic teaching.`;
}

// ─── Major witness builder ────────────────────────────────────────────────────

async function buildMajorWitnesses(
  expandedTopics: Array<{ id: number; name: string }>,
  salienceTopicIds: number[],
  topicRelevanceWeight: Map<number, number>,
  semanticBooks: Array<{ book_id: number; score: number }>,
  semanticCoords: Map<
    string,
    {
      book_id: number;
      chapter: number;
      verse: number;
      translation_id: number;
      score: number;
    }
  >,
  semanticVerseMap: Map<string, VerseRow>,
): Promise<MajorWitness[]> {
  const topicIds = expandedTopics.map((t) => t.id);

  // Build book semantic score lookup from vectorize book results.
  const bookSemanticScoreMap = new Map<number, number>();
  for (const { book_id, score } of semanticBooks) {
    bookSemanticScoreMap.set(book_id, score);
  }

  const candidates = await aggregateWitnesses(topicIds);

  // Count semantic verse hits per book — direct embedding-based relevance signal.
  const semanticHitsPerBook = new Map<number, number>();
  for (const coord of semanticCoords.values()) {
    semanticHitsPerBook.set(
      coord.book_id,
      (semanticHitsPerBook.get(coord.book_id) ?? 0) + 1,
    );
  }

  // DEBUG: log candidate details
  console.error(`[DEBUG] semanticHitsPerBook: ${Array.from(semanticHitsPerBook.entries()).map(([k, v]) => `${k}:${v}`).join(', ')}`);
  console.error(`[DEBUG] expandedTopics count: ${expandedTopics.length}, topicNames: ${expandedTopics.slice(0, 15).map(t => t.name).join(', ')}`);
  console.error(`[DEBUG] candidates (${candidates.length} total): ${candidates.map(c => `${c.book_name}:vc=${c.verse_count}`).join(' | ')}`);
  console.error(`[DEBUG] Job in candidates: ${candidates.find(c => c.book_name === 'Job') ? 'YES' : 'NO'}`);

  // Fetch salience using the broad topic set (including co-occurring topics) and
  // actual candidate book IDs. The broad set ensures we capture salience signals
  // from related categories (e.g., AFFLICTIONS for SUFFERING queries) that the
  // narrow seed topics alone would miss.
  const candidateBookIds = candidates.map((c) => c.book_id);
  const [salienceMap, anchorPassageData] = await Promise.all([
    fetchSalience(salienceTopicIds, candidateBookIds),
    fetchAnchorPassageData(topicIds, candidateBookIds),
  ]);

  // Filter to qualified witnesses only.
  const qualified = candidates.filter(
    (c) =>
      c.verse_count >= MAJOR_WITNESS_MIN_VERSES &&
      c.chapter_count >= MAJOR_WITNESS_MIN_CHAPTERS,
  );

  // Score candidates using five complementary signals:
  //   1. Salience (pre-computed topical centrality) — dominant when available
  //   2. Chapter coverage ratio (matched chapters / total book chapters) —
  //      rewards books where the topic pervades the whole text
  //   3. log2(verse_count) — breadth tiebreaker
  //   4. Book semantic score — Vectorize book-level embedding match
  //   5. Semantic verse hits — how many of the top verse-level embedding
  //      results land in this book (direct query-relevance signal)
  const scored = qualified.map((candidate) => {
    const bookSemanticScore = bookSemanticScoreMap.get(candidate.book_id) ?? 0;

    // Sum relevance-weighted salience across all seed topics for this book.
    // Each topic's salience contribution is multiplied by its relevance to
    // the query (1.0 for LIKE matches, semantic score for Vectorize topics).
    // This prevents low-relevance semantic topics (e.g., JEHOASH for "end
    // times prophecy") from dominating the score for specific books (2 Kings).
    let totalSalience = 0;
    for (const topicId of salienceTopicIds) {
      const sal = salienceMap.get(`${candidate.book_id}:${topicId}`) ?? 0;
      const weight = topicRelevanceWeight.get(topicId) ?? 0.5;
      totalSalience += sal * weight;
    }

    // Chapter coverage: what fraction of this book's chapters contain
    // topical verses? High coverage = the topic is central to the book.
    const totalBookChapters = BOOK_TOTAL_CHAPTERS[candidate.book_name] ?? 1;
    const chapterCoverage = candidate.chapter_count / totalBookChapters;

    // Semantic verse hits: number of top Vectorize verse matches in this
    // book. A book with many semantically similar verses is directly
    // relevant to the query even when Nave's topic expansion misses it.
    const semVerseHits = semanticHitsPerBook.get(candidate.book_id) ?? 0;

    const verseBreadth = Math.log2(candidate.verse_count + 1);
    const witnessScore =
      totalSalience * 3.0 +
      chapterCoverage * 4.0 +
      verseBreadth +
      bookSemanticScore * 3.0 +
      semVerseHits * 1.5;
    return { candidate, witnessScore };
  });

  scored.sort((a, b) => b.witnessScore - a.witnessScore);
  // DEBUG: log scored results
  console.error(`[DEBUG] scored (top 15): ${scored.slice(0, 15).map(s => `${s.candidate.book_name}: ws=${s.witnessScore.toFixed(2)}, vc=${s.candidate.verse_count}, cc=${s.candidate.chapter_count}, sem=${(bookSemanticScoreMap.get(s.candidate.book_id) ?? 0).toFixed(3)}, sal=${(() => { let tot = 0; for (const tid of salienceTopicIds) { tot += salienceMap.get(s.candidate.book_id + ':' + tid) ?? 0; } return tot.toFixed(3); })()}, svh=${semanticHitsPerBook.get(s.candidate.book_id) ?? 0}`).join(' | ')}`);
  // Apply both a hard cap and a relative score cutoff so that:
  // - Queries with a clear top-N (large score gaps) return fewer witnesses
  // - Queries with many similarly-scored books (like "end times prophecy")
  //   return more witnesses to capture all relevant books
  const topScore = scored.length > 0 ? scored[0].witnessScore : 0;
  const cutoff = topScore * WITNESS_SCORE_CUTOFF;
  const topCandidates = scored
    .slice(0, MAX_MAJOR_WITNESSES)
    .filter((s) => s.witnessScore >= cutoff)
    .map((s) => s.candidate);

  // Build witnesses concurrently (representative verse may need D1 fallback).
  const witnesses: MajorWitness[] = await Promise.all(
    topCandidates.map(async (candidate) => {
      const matchedTopics = candidate.topic_names
        .split(',')
        .map((n) => n.trim())
        .filter(Boolean);

      // Prefer short topic names as narrative labels.
      const shortTopics = matchedTopics.filter(
        (t) => t.split(/\s+/).length <= 3,
      );
      const narrative = detectNarrative(candidate, shortTopics);

      const representativeVerse = await fetchRepresentativeVerse(
        candidate,
        semanticCoords,
        semanticVerseMap,
      );

      // Build enrichment fields from in-memory data (no additional D1 queries).
      const whyThisBookMatters = buildWhyThisBookMatters(
        candidate,
        salienceMap,
        salienceTopicIds,
        expandedTopics,
      );

      const themesMatched = buildThemesMatched(
        candidate,
        expandedTopics,
        salienceMap,
        salienceTopicIds,
      );

      const totalBookChapters = BOOK_TOTAL_CHAPTERS[candidate.book_name] ?? candidate.chapter_count;
      const anchorRows = anchorPassageData.get(candidate.book_id) ?? [];
      const suggestedAnchorPassages = clusterToPassageRanges(
        anchorRows,
        candidate.book_name,
        totalBookChapters,
      );

      const narrativeReason = buildNarrativeReason(narrative, candidate);

      const witness: MajorWitness = {
        book: candidate.book_name,
        testament: candidate.testament,
        verse_count: candidate.verse_count,
        chapter_count: candidate.chapter_count,
        matched_topics: matchedTopics.slice(0, 10),
        match_reason: buildWitnessMatchReason(candidate, narrative),
        representative_verse: representativeVerse,
        why_this_book_matters: whyThisBookMatters,
        themes_matched: themesMatched,
        suggested_anchor_passages: suggestedAnchorPassages,
      };

      if (narrative !== undefined) {
        witness.narrative = narrative;
      }

      if (narrativeReason !== undefined) {
        witness.narrative_reason = narrativeReason;
      }

      return witness;
    }),
  );

  return witnesses;
}

// ─── Nave's D1 search ─────────────────────────────────────────────────────────

interface NaveSearchEntry {
  result: TopicalResult;
  relevance: number;
}

async function searchNaves(
  topic: string,
  limit: number,
): Promise<Map<string, NaveSearchEntry>> {
  // Normalize the topic the same way the ETL does: lowercase.
  const normalized = topic.toLowerCase();

  // Escape SQLite LIKE metacharacters to prevent callers from using % or _
  // to enumerate all topics.
  const escaped = normalized.replace(/%/g, '\\%').replace(/_/g, '\\_');

  // Resolve translation ID from cache; fall back to JOIN on abbreviation if
  // the cache is not yet populated (e.g. missing env vars at startup).
  const kjvTranslation = getTranslation('KJV');
  const translationFilter = kjvTranslation
    ? `AND v.translation_id = ?`
    : `AND t.abbreviation = 'KJV'`;
  const translationParams: unknown[] = kjvTranslation ? [kjvTranslation.id] : [];

  // Use LIKE with % wildcards for partial matching so 'forgive' matches
  // 'forgiveness', 'forgiven', etc. Also try exact match first via UNION.
  const result = await d1.query(
    `SELECT
       b.name           AS book_name,
       ntv.chapter      AS chapter,
       ntv.verse        AS verse,
       t.abbreviation   AS translation_abbrev,
       v.text           AS text,
       ntv.note         AS note,
       nt.topic_name    AS topic_name
     FROM nave_topic_verses ntv
     JOIN nave_topics nt  ON nt.id = ntv.topic_id
     JOIN books b         ON b.id  = ntv.book_id
     JOIN verses v        ON v.book_id       = ntv.book_id
                         AND v.chapter       = ntv.chapter
                         AND v.verse         = ntv.verse
                         ${translationFilter}
     JOIN translations t  ON t.id = v.translation_id
     WHERE nt.normalized_topic LIKE ? ESCAPE '\\'
     ORDER BY b.canonical_order, ntv.chapter, ntv.verse
     LIMIT ?`,
    [...translationParams, `%${escaped}%`, limit * 3],
  );

  const resultMap = new Map<string, NaveSearchEntry>();

  for (const row of result.results) {
    const r = row as unknown as NaveVerseRow;
    const key = `${r.book_name.trim().toLowerCase()}:${r.chapter}:${r.verse}`;

    // Keep the entry with highest relevance when the same verse appears under
    // multiple matching topics.
    const relevance = naveTopicRelevance(r.topic_name, topic);
    if (resultMap.has(key)) {
      const existing = resultMap.get(key)!;
      if (relevance <= existing.relevance) continue;
    }

    const citation: Citation = {
      book: r.book_name,
      chapter: r.chapter,
      verse: r.verse,
      translation: r.translation_abbrev,
    };

    const matchReason = `Nave's Topical Bible: ${r.topic_name}`;

    const entry: TopicalResult = {
      text: r.text,
      citation,
      source: 'naves',
      match_reason: matchReason,
    };

    if (r.note) {
      entry.note = r.note;
    }

    resultMap.set(key, { result: entry, relevance });
  }

  return resultMap;
}

// ─── Vectorize semantic search ─────────────────────────────────────────────────

// Accepts a pre-computed embedding vector (avoids redundant Workers AI calls).
async function searchSemanticFromVector(
  queryVector: number[],
  limit: number,
): Promise<Map<string, { book_id: number; chapter: number; verse: number; translation_id: number; score: number }>> {
  const topK = Math.min(limit * VECTORIZE_OVERFETCH_MULTIPLIER, 20);
  const matches = await vectorize.query(queryVector, { topK });
  if (matches.length === 0) return new Map();

  // Each match metadata contains book_id, chapter, verse, translation_id.
  const coordMap = new Map<string, { book_id: number; chapter: number; verse: number; translation_id: number; score: number }>();

  for (const match of matches) {
    const meta = match.metadata;
    if (!meta) continue;

    const bookId = meta['book_id'] as number;
    const chapter = meta['chapter'] as number;
    const verse = meta['verse'] as number;
    const translationId = meta['translation_id'] as number;

    if (!bookId || !chapter || !verse || !translationId) continue;

    // Use book_id:chapter:verse as a translation-agnostic dedup key.
    // We'll fetch the actual book name via D1 query below.
    const key = `${bookId}:${chapter}:${verse}`;
    if (!coordMap.has(key)) {
      coordMap.set(key, { book_id: bookId, chapter, verse, translation_id: translationId, score: match.score });
    }
  }

  return coordMap;
}

// ─── Fetch verse text for semantic results ─────────────────────────────────────

// D1 limits bound parameters to 100 per statement. Each verse lookup requires
// 4 parameters (book_id, chapter, verse, translation_id), so cap chunk size at 25.
const VERSE_CHUNK_SIZE = 25;

const VERSE_SELECT_SQL = `SELECT
       v.book_id         AS book_id,
       v.chapter         AS chapter,
       v.verse           AS verse,
       b.name            AS book_name,
       t.abbreviation    AS translation_abbrev,
       v.text            AS text
     FROM verses v
     JOIN books b       ON b.id = v.book_id
     JOIN translations t ON t.id = v.translation_id
     WHERE `;

function buildVerseChunkStatement(
  chunk: Array<{ book_id: number; chapter: number; verse: number; translation_id: number }>,
): { sql: string; params: unknown[] } {
  const clauses = chunk
    .map(() => '(v.book_id = ? AND v.chapter = ? AND v.verse = ? AND v.translation_id = ?)')
    .join(' OR ');

  const params: unknown[] = [];
  for (const c of chunk) {
    params.push(c.book_id, c.chapter, c.verse, c.translation_id);
  }

  return { sql: `${VERSE_SELECT_SQL}${clauses}`, params };
}

async function fetchVerseTexts(
  coords: Array<{ book_id: number; chapter: number; verse: number; translation_id: number }>,
): Promise<Map<string, VerseRow>> {
  if (coords.length === 0) return new Map();

  // Chunk coords to stay within D1's 100-parameter-per-statement ceiling.
  const chunks: Array<typeof coords> = [];
  for (let i = 0; i < coords.length; i += VERSE_CHUNK_SIZE) {
    chunks.push(coords.slice(i, i + VERSE_CHUNK_SIZE));
  }

  // Issue all chunk queries concurrently.
  const statements = chunks.map(buildVerseChunkStatement);
  const resultSets = await Promise.all(
    statements.map((stmt) => d1.query(stmt.sql, stmt.params))
  );

  const verseMap = new Map<string, VerseRow>();

  for (const resultSet of resultSets) {
    for (const row of resultSet.results) {
      const r = row as unknown as VerseRow;
      // Key uses book_id (numeric) to match the coordMap keys from semantic search.
      const key = `${r.book_id}:${r.chapter}:${r.verse}`;
      verseMap.set(key, r);
    }
  }

  return verseMap;
}

// ─── Consecutive verse cap ────────────────────────────────────────────────────

// Caps Nave's results to at most 2 consecutive verses per book:chapter group.
// "Consecutive" means verse numbers are sequential (e.g., 3,4 or 7,8).
// This prevents a dense Nave's cluster from monopolizing the result set.
function capConsecutiveVerses(
  navesMap: Map<string, NaveSearchEntry>,
): Map<string, NaveSearchEntry> {
  // Group entries by book:chapter.
  const byChapter = new Map<string, Array<{ key: string; verse: number; entry: NaveSearchEntry }>>();

  for (const [key, entry] of navesMap) {
    const { book, chapter, verse } = entry.result.citation;
    const chapterKey = `${book.trim().toLowerCase()}:${chapter}`;
    if (!byChapter.has(chapterKey)) {
      byChapter.set(chapterKey, []);
    }
    byChapter.get(chapterKey)!.push({ key, verse, entry });
  }

  const capped = new Map<string, NaveSearchEntry>();

  for (const group of byChapter.values()) {
    // Sort by verse number so consecutive detection is straightforward.
    group.sort((a, b) => a.verse - b.verse);

    let consecutiveRun = 1;
    let prevVerse: number | null = null;

    for (const item of group) {
      if (prevVerse !== null && item.verse === prevVerse + 1) {
        consecutiveRun++;
      } else {
        consecutiveRun = 1;
      }

      if (consecutiveRun <= 2) {
        capped.set(item.key, item.entry);
      }

      prevVerse = item.verse;
    }
  }

  return capped;
}

// ─── Tool handler ─────────────────────────────────────────────────────────────

const topicalSearch: ToolHandler = async (args, _ask?) => {
  await ensureInitialized();

  const { topic, limit: limitArg } = args as { topic: string; limit?: number };
  const limit = Math.min(Math.max(limitArg ?? 20, 1), 50);

  // Phase 1: Embed query once, fire Nave's search concurrently.
  const embeddingPromise = workersAi.embed([topic]);
  const [embeddings, navesResults] = await Promise.all([
    embeddingPromise,
    searchNaves(topic, limit),
  ]);

  const queryVector = embeddings.length > 0 ? embeddings[0] : [];

  // Phase 2: All Vectorize queries use same embedding, run concurrently.
  // Graceful degradation: if topic index is not configured, searchSemanticTopicsAndBooks
  // returns empty arrays — fall back to Nave's LIKE for witnesses.
  // Topic + book results share one Vectorize query (saves an HTTP round-trip).
  const semanticCoordsPromise = queryVector.length > 0
    ? searchSemanticFromVector(queryVector, limit)
    : Promise.resolve(new Map<string, { book_id: number; chapter: number; verse: number; translation_id: number; score: number }>());

  const semanticTopicsAndBooksPromise = queryVector.length > 0
    ? searchSemanticTopicsAndBooks(queryVector)
    : Promise.resolve({ topics: [] as Array<{ id: number; name: string; score: number }>, books: [] as Array<{ book_id: number; score: number }> });

  // Run LIKE topic expansion in parallel with Vectorize queries to avoid
  // sequential latency. Both sources are needed for comprehensive coverage.
  const likeTopicsPromise = queryExpandedTopicsByLike(topic);

  const [{ topics: semanticTopics, books: semanticBooks }, semanticCoords, likeTopics] = await Promise.all([
    semanticTopicsAndBooksPromise,
    semanticCoordsPromise,
    likeTopicsPromise,
  ]);

  console.error(`[DEBUG] semanticTopics: ${semanticTopics.map(t => `${t.name}(${t.id},s=${t.score.toFixed(3)})`).join(', ')}`);
  console.error(`[DEBUG] likeTopics: ${likeTopics.map(t => `${t.name}(${t.id})`).join(', ')}`);

  const semanticVerseMap = await fetchVerseTexts(Array.from(semanticCoords.values()));

  // Merge semantic topics with LIKE-based topics for comprehensive coverage.
  // Semantic topics find conceptually related categories (e.g., SUFFERING, PAIN)
  // while LIKE finds keyword matches (e.g., AFFLICTIONS AND ADVERSITIES).
  // Both are needed: narrow semantic topics alone miss rich keyword categories.
  const topicById = new Map<number, { id: number; name: string }>();
  for (const t of semanticTopics) topicById.set(t.id, t);
  for (const t of likeTopics) {
    if (!topicById.has(t.id)) topicById.set(t.id, t);
  }
  const expandedTopics: Array<{ id: number; name: string }> = Array.from(topicById.values());

  // Expand via co-occurrence to discover related categories (e.g., AFFLICTIONS
  // for SUFFERING queries). Co-occurring topics with high salience for specific
  // books are included in the aggregation; all co-occurring topics contribute
  // to salience scoring.
  const seedTopicIds = expandedTopics.map((t) => t.id);
  const cooccurringTopics = await expandTopicsByCooccurrence(seedTopicIds);
  console.error(`[DEBUG] cooccurringTopics: ${cooccurringTopics.map(t => `${t.name}(${t.id})`).join(', ')}`);

  // Fetch salience for co-occurring topics to identify which are topically
  // concentrated (high salience = canonical for a specific book) vs spread
  // evenly (generic topics that would inflate all books equally).
  const coTopicIds = cooccurringTopics.map((t) => t.id);
  const coSalienceEntries = coTopicIds.length > 0
    ? await fetchCooccurringSalience(coTopicIds)
    : [];

  // Include co-occurring topics in aggregation ONLY if they are among the
  // top co-occurring topics by shared verse count AND have high salience
  // for at least one book. This dual filter ensures we include AFFLICTIONS
  // (high overlap with SUFFERING + high salience for Job) but exclude
  // generic categories like MINISTER or COMMANDMENTS (high salience but
  // low thematic overlap with the query).
  const highSalienceTopicIds = new Set<number>();
  for (const entry of coSalienceEntries) {
    highSalienceTopicIds.add(entry.topic_id);
  }

  // Promote co-occurring topics with high salience into the aggregation.
  // Co-occurring topics are already sorted by shared verse count (most
  // overlapping first), so the first few are the most thematically relevant.
  // Cap at 3 to avoid inflating the topic set with loosely related categories.
  let promoted = 0;
  const MAX_PROMOTED = 3;
  for (const t of cooccurringTopics) {
    if (promoted >= MAX_PROMOTED) break;
    if (highSalienceTopicIds.has(t.id) && !topicById.has(t.id)) {
      topicById.set(t.id, t);
      promoted++;
    }
  }

  // Rebuild expandedTopics to include promoted co-occurring topics.
  const finalExpandedTopics: Array<{ id: number; name: string }> = Array.from(topicById.values());

  // Only seed topics (semantic + LIKE) contribute to salience scoring.
  // Co-occurring topics are used for verse aggregation but NOT for salience,
  // because they introduce noise (e.g., MINISTER inflating 2 Timothy for
  // a "suffering" query).
  //
  // Each seed topic carries a relevance weight:
  //   - LIKE-matched topics: 1.0 (direct keyword match)
  //   - Semantic-only topics: their Vectorize similarity score (0-1)
  // This prevents low-relevance semantic topics (e.g., JEHOASH for "end
  // times prophecy") from inflating salience for specific books.
  const salienceTopicIds = seedTopicIds;
  const likeTopicIdSet = new Set(likeTopics.map((t) => t.id));
  const semanticScoreById = new Map<number, number>();
  for (const t of semanticTopics) {
    semanticScoreById.set(t.id, t.score);
  }
  const topicRelevanceWeight = new Map<number, number>();
  for (const tid of seedTopicIds) {
    if (likeTopicIdSet.has(tid)) {
      topicRelevanceWeight.set(tid, 1.0);
    } else {
      topicRelevanceWeight.set(tid, semanticScoreById.get(tid) ?? 0.5);
    }
  }

  console.error(`[DEBUG] finalExpandedTopics: ${finalExpandedTopics.map(t => t.name).join(', ')}`);
  console.error(`[DEBUG] promoted co-occurring: ${promoted}`);

  // Phase 3 (was Phase 2): Build major witnesses using semantic topics + book scores + salience.
  const majorWitnesses =
    finalExpandedTopics.length > 0
      ? await buildMajorWitnesses(finalExpandedTopics, salienceTopicIds, topicRelevanceWeight, semanticBooks, semanticCoords, semanticVerseMap)
      : [];

  // Phase 4: Existing merge + witness interleave + match reasons.

  // Apply consecutive verse cap to Nave's results before merge.
  const navesResultsCapped = capConsecutiveVerses(navesResults);

  // Build semantic TopicalResults with score lookup.
  // coordMap uses book_id:chapter:verse keys; build a unified-key score map via verseMap.
  const semanticScoreByKey = new Map<string, number>();
  for (const [coordKey, coord] of semanticCoords) {
    const verseRow = semanticVerseMap.get(coordKey);
    if (!verseRow) continue;
    const unifiedKey = `${verseRow.book_name.trim().toLowerCase()}:${verseRow.chapter}:${verseRow.verse}`;
    semanticScoreByKey.set(unifiedKey, coord.score);
  }

  const semanticEntries: Array<{ unifiedKey: string; result: TopicalResult }> = [];

  for (const [coordKey] of semanticCoords) {
    const verseRow = semanticVerseMap.get(coordKey);
    if (!verseRow) continue;

    const unifiedKey = `${verseRow.book_name.trim().toLowerCase()}:${verseRow.chapter}:${verseRow.verse}`;

    if (navesResultsCapped.has(unifiedKey)) {
      // Mark as 'both' in the Nave's map and upgrade match_reason.
      const existing = navesResultsCapped.get(unifiedKey)!;
      existing.result.source = 'both';
      const topicName = existing.result.match_reason?.replace("Nave's Topical Bible: ", '') ?? '';
      existing.result.match_reason = topicName
        ? `Nave's Topical Bible: ${topicName} + Semantic similarity`
        : 'Nave\'s Topical Bible + Semantic similarity';
    } else {
      const citation: Citation = {
        book: verseRow.book_name,
        chapter: verseRow.chapter,
        verse: verseRow.verse,
        translation: verseRow.translation_abbrev,
      };

      semanticEntries.push({
        unifiedKey,
        result: {
          text: verseRow.text,
          citation,
          source: 'semantic',
          match_reason: 'Semantic similarity to query',
        },
      });
    }
  }

  // Sort semantic-only entries by descending vector score.
  semanticEntries.sort((a, b) => {
    const scoreA = semanticScoreByKey.get(a.unifiedKey) ?? 0;
    const scoreB = semanticScoreByKey.get(b.unifiedKey) ?? 0;
    return scoreB - scoreA;
  });

  // Deduplicate semantic entries (a unified key may appear multiple times if
  // multiple translations matched in the overfetch pool).
  const uniqueSemanticEntries: Array<{ unifiedKey: string; result: TopicalResult }> = [];
  const semanticSeen = new Set<string>();
  for (const entry of semanticEntries) {
    if (!semanticSeen.has(entry.unifiedKey)) {
      semanticSeen.add(entry.unifiedKey);
      uniqueSemanticEntries.push(entry);
    }
  }

  // Split Nave's results into 'both' (matched semantic too) and 'naves'-only.
  const bothEntries: Array<{ unifiedKey: string; result: TopicalResult }> = [];
  const navesOnlyEntries: Array<{ unifiedKey: string; relevance: number; result: TopicalResult }> = [];

  for (const [key, entry] of navesResultsCapped) {
    if (entry.result.source === 'both') {
      bothEntries.push({ unifiedKey: key, result: entry.result });
    } else {
      navesOnlyEntries.push({ unifiedKey: key, relevance: entry.relevance, result: entry.result });
    }
  }

  // Sort 'both' entries by descending semantic score — strongest combined signal first.
  bothEntries.sort((a, b) => {
    const scoreA = semanticScoreByKey.get(a.unifiedKey) ?? 0;
    const scoreB = semanticScoreByKey.get(b.unifiedKey) ?? 0;
    return scoreB - scoreA;
  });

  // Sort Nave's-only entries by descending topic relevance score.
  navesOnlyEntries.sort((a, b) => b.relevance - a.relevance);

  // Adaptive budget split:
  //   - 'both' entries always come first (capped at limit).
  //   - Remaining slots use a 60/40 split between Nave's-only and semantic.
  //   - If Nave's-only has fewer results than its budget, ALL remaining slots
  //     go to semantic (and vice versa).
  const remainingAfterBoth = Math.max(0, limit - bothEntries.length);
  const navesOnlyBudget = Math.round(remainingAfterBoth * 0.6);
  const semanticBudget = remainingAfterBoth - navesOnlyBudget;

  const navesOnlyPool = navesOnlyEntries.map((e) => e.result);
  const semanticPool = uniqueSemanticEntries.map((e) => e.result);

  const navesOnlySlice = navesOnlyPool.slice(0, navesOnlyBudget);
  const semanticSlice = semanticPool.slice(0, semanticBudget);

  // Graceful degradation: unused slots from either source backfill from the other.
  const naveOnlyOverflow = navesOnlyBudget - navesOnlySlice.length;
  const semanticOverflow = semanticBudget - semanticSlice.length;

  const extraSemantic = naveOnlyOverflow > 0
    ? semanticPool.slice(semanticBudget, semanticBudget + naveOnlyOverflow)
    : [];
  const extraNaves = semanticOverflow > 0
    ? navesOnlyPool.slice(navesOnlyBudget, navesOnlyBudget + semanticOverflow)
    : [];

  // Final ordering: 'both' first (by semantic score), then remaining by relevance.
  const mergedResults = [
    ...bothEntries.map((e) => e.result),
    ...navesOnlySlice,
    ...extraNaves,
    ...semanticSlice,
    ...extraSemantic,
  ].slice(0, limit);

  // Soft-interleave witness-book verses if major witness books are underrepresented.
  // Underrepresented = witness book verses make up less than 15% of results.
  let results = mergedResults;

  if (majorWitnesses.length > 0) {
    // We need candidate book_ids for the interleave check — re-use aggregation
    // data already computed. To avoid another D1 round-trip, derive book_ids
    // from semanticCoords and verseMap that we already have in memory.
    const witnessBookNames = new Set(
      majorWitnesses.map((w) => w.book.toLowerCase()),
    );

    const witnessVerseCount = mergedResults.filter((r) =>
      witnessBookNames.has(r.citation.book.toLowerCase()),
    ).length;

    const witnessRatio =
      mergedResults.length > 0 ? witnessVerseCount / mergedResults.length : 0;

    if (witnessRatio < 0.15) {
      // Collect witness-book verse candidates from semantic pool not already in results.
      const existingKeys = new Set(
        mergedResults.map(
          (r) =>
            `${r.citation.book.trim().toLowerCase()}:${r.citation.chapter}:${r.citation.verse}`,
        ),
      );

      const witnessVerseCandidates: TopicalResult[] = [];
      const candidatePools: TopicalResult[] = [
        ...uniqueSemanticEntries.map((e) => e.result),
        ...navesOnlyEntries.map((e) => e.result),
      ];
      for (const result of candidatePools) {
        if (!witnessBookNames.has(result.citation.book.toLowerCase())) continue;
        const k = `${result.citation.book.trim().toLowerCase()}:${result.citation.chapter}:${result.citation.verse}`;
        if (existingKeys.has(k)) continue;
        witnessVerseCandidates.push(result);
      }

      if (witnessVerseCandidates.length > 0) {
        const interleaved: TopicalResult[] = [];
        let witnessIdx = 0;

        for (let i = 0; i < mergedResults.length; i++) {
          interleaved.push(mergedResults[i]);
          if (
            (i + 1) % WITNESS_INTERLEAVE_INTERVAL === 0 &&
            witnessIdx < witnessVerseCandidates.length
          ) {
            interleaved.push(witnessVerseCandidates[witnessIdx++]);
          }
        }

        results = interleaved.slice(0, limit);
      }
    }
  }

  const response: TopicalSearchResponse = {
    topic,
    results,
    total_results: results.length,
    major_witnesses: majorWitnesses,
  };

  return response;
};

topicalSearch.annotations = {
  readOnlyHint: true,
  destructiveHint: false,
  idempotentHint: true,
  openWorldHint: true,
};

topicalSearch.description =
  'Research what the Bible teaches about a topic. Best for broad theological questions, especially "What does the Bible say about X?" queries. ' +
  'Returns both direct verse hits and major_witnesses: the books, narratives, and passages that are the Bible\'s principal treatments of the topic. ' +
  'Combines Nave\'s curated Topical Bible (5,319 categories) with AI semantic search. ' +
  'Works for single topics ("forgiveness", "prayer") and compound themes ("God\'s faithfulness during suffering", "hope in the face of death"). ' +
  'Prefer this over semantic_search when the answer should include major biblical witnesses across passages, narratives, books, or genres. ' +
  '(Note: For narrative-heavy witnesses, narrative_reason provides additional context beyond match_reason.)';

topicalSearch.input = {
  topic: T.string({
    required: true,
    description:
      'The topic to search for (e.g., "forgiveness", "prayer", "faith", "love"). ' +
      'Nave\'s index is searched by keyword match; semantic search finds conceptually related verses.',
    minLength: 1,
  }),
  limit: T.number({
    required: false,
    description: 'Maximum number of results to return (default 20, max 50).',
    min: 1,
    max: 50,
    default: 20,
  }),
};

export default topicalSearch;
