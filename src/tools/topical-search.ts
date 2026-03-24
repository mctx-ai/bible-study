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

import type { ToolHandler } from '@mctx-ai/app';
import { T } from '@mctx-ai/app';
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
  genre: string;
  verse_count: number;
  chapter_count: number;
  matched_topics: string[];
  narrative?: string;
  match_reason: string;
  witness_strength: 'central' | 'strong' | 'supporting';
  representative_verse: {
    text: string;
    citation: Citation;
  };
  why_this_book_matters?: string;
  themes_matched?: string[];
  suggested_anchor_passages?: string[];
  narrative_reason?: string;
  query_alignment_note?: string;
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
// In narrative mode, non-primary books need higher thresholds to qualify.
// This prevents loosely related books (e.g. Exodus, Numbers for a Noah query)
// from appearing as witnesses when they only carry a handful of tangential verses.
const MAJOR_WITNESS_MIN_VERSES_NARRATIVE_NON_PRIMARY = 8;
const MAJOR_WITNESS_MIN_CHAPTERS_NARRATIVE_NON_PRIMARY = 3;
const MAX_MAJOR_WITNESSES = 12;
// For narrative-mode queries (specific stories/figures), cap witnesses tightly.
const MAX_MAJOR_WITNESSES_NARRATIVE = 5;
// Witnesses scoring below this fraction of the top witness score are excluded,
// even if fewer than MAX_MAJOR_WITNESSES have been emitted.
// Raised from 0.55 to 0.60 to reduce noise from weakly related witnesses
// on broad theological queries (e.g. Isaiah appearing for suffering queries
// primarily due to high chapter coverage in prophetic genre rather than
// direct topical relevance). The increase is intentionally moderate — raising
// too high (e.g. 0.65) removes legitimate witnesses for queries like "end
// times prophecy" where many books score similarly.
const WITNESS_SCORE_CUTOFF = 0.60;
// Narrative queries use a stricter cutoff — only genuinely related secondary
// witnesses survive (e.g. Hebrews 11, 1 Peter 3 for Noah — not Exodus, Numbers).
const WITNESS_SCORE_CUTOFF_NARRATIVE = 0.75;
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

// ─── Literary genre lookup for all 66 canonical books ────────────────────────

// Maps each canonical Bible book name to its literary genre category.
// Categories: Law, History, Wisdom, Poetry, Prophecy, Gospel, Acts, Epistle, Apocalyptic
const BOOK_GENRE: Record<string, string> = {
  // Old Testament — Law (Pentateuch)
  Genesis: 'Law',
  Exodus: 'Law',
  // Leviticus and Numbers are Law books but contain mostly ritual/legal content
  // with little narrative arc; density clustering (default) is more appropriate
  // than arc clustering, so they are classified as 'Wisdom' to route them there.
  Leviticus: 'Wisdom',
  Numbers: 'Wisdom',
  Deuteronomy: 'Law',
  // Old Testament — History
  Joshua: 'History',
  Judges: 'History',
  Ruth: 'History',
  '1 Samuel': 'History',
  '2 Samuel': 'History',
  '1 Kings': 'History',
  '2 Kings': 'History',
  '1 Chronicles': 'History',
  '2 Chronicles': 'History',
  Ezra: 'History',
  Nehemiah: 'History',
  Esther: 'History',
  // Old Testament — Wisdom
  Job: 'Wisdom',
  Proverbs: 'Wisdom',
  Ecclesiastes: 'Wisdom',
  // Old Testament — Poetry
  Psalms: 'Poetry',
  Lamentations: 'Poetry',
  'Song of Solomon': 'Poetry',
  // Old Testament — Prophecy
  Isaiah: 'Prophecy',
  Jeremiah: 'Prophecy',
  Ezekiel: 'Prophecy',
  Daniel: 'Apocalyptic',
  Hosea: 'Prophecy',
  Joel: 'Prophecy',
  Amos: 'Prophecy',
  Obadiah: 'Prophecy',
  Jonah: 'Prophecy',
  Micah: 'Prophecy',
  Nahum: 'Prophecy',
  Habakkuk: 'Prophecy',
  Zephaniah: 'Prophecy',
  Haggai: 'Prophecy',
  Zechariah: 'Prophecy',
  Malachi: 'Prophecy',
  // New Testament — Gospel
  Matthew: 'Gospel',
  Mark: 'Gospel',
  Luke: 'Gospel',
  John: 'Gospel',
  // New Testament — Acts
  Acts: 'Acts',
  // New Testament — Epistle
  Romans: 'Epistle',
  '1 Corinthians': 'Epistle',
  '2 Corinthians': 'Epistle',
  Galatians: 'Epistle',
  Ephesians: 'Epistle',
  Philippians: 'Epistle',
  Colossians: 'Epistle',
  '1 Thessalonians': 'Epistle',
  '2 Thessalonians': 'Epistle',
  '1 Timothy': 'Epistle',
  '2 Timothy': 'Epistle',
  Titus: 'Epistle',
  Philemon: 'Epistle',
  Hebrews: 'Epistle',
  James: 'Epistle',
  '1 Peter': 'Epistle',
  '2 Peter': 'Epistle',
  '1 John': 'Epistle',
  '2 John': 'Epistle',
  '3 John': 'Epistle',
  Jude: 'Epistle',
  // New Testament — Apocalyptic
  Revelation: 'Apocalyptic',
};

// ─── Nave's topic relevance scoring ───────────────────────────────────────────

// Returns a [0, 1] relevance score for a Nave's topic name against the query.
// Exact match → 1.0; topic name starts with query → 0.8; query is a word in
// topic name → 0.6; topic name contains query → 0.4; partial substring → 0.2.
// This gives a simple but meaningful ordering within the Nave's-only pool.
//
// Punctuation (apostrophes, hyphens, etc.) is stripped before word-boundary
// checks so possessive topic names like "NOAH'S ARK" score 0.6 (word match)
// rather than 0.4 (substring match) for a "Noah" query.
function naveTopicRelevance(topicName: string, query: string): number {
  // Strip punctuation before comparison to handle possessive names (e.g. "NOAH'S ARK").
  const stripPunct = (s: string) => s.replace(/[^a-z0-9\s]/g, '');
  const t = stripPunct(topicName.toLowerCase());
  const q = stripPunct(query.toLowerCase());
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

// ─── Query-level narrative detection ─────────────────────────────────────────

// Describes a detected narrative query: the canonical figure/story name and
// the primary book where the narrative lives (if determinable from LIKE topics).
interface NarrativeContext {
  narrativeMode: true;
  narrativeFigure: string;
  // book_id of the primary narrative book (highest verse_count for the figure topic),
  // populated after witness candidates are available. Initially undefined.
  primaryBookId?: number;
}

// Well-known biblical proper nouns used to anchor narrative detection via regex.
// These are figures/stories that have specific, bounded narratives in one primary book.
// Note: 'Ark' is intentionally excluded — 'Noah' already anchors Noah queries,
// and standalone 'Ark' can falsely trigger narrative mode on thematic queries
// like 'covenant and ark' (which should match both narratives broadly).
const BIBLICAL_PROPER_NOUN_PATTERN =
  /\b(Noah|Abraham|Isaac|Jacob|Joseph|Moses|Burning Bush|Red Sea|Exodus|Joshua|Rahab|Caleb|Samson|Gideon|Deborah|Ruth|Samuel|David|Goliath|Solomon|Elijah|Elisha|Jonah|Esther|Daniel|Shadrach|Meshach|Abednego|Nebuchadnezzar|Ezra|Nehemiah|Job|Mary|Joseph of Nazareth|Nativity|Baptism of Jesus|Transfiguration|Lazarus|Zacchaeus|Prodigal Son|Good Samaritan|Feeding of the Five Thousand|Triumphal Entry|Last Supper|Gethsemane|Crucifixion|Resurrection|Pentecost|Paul|Saul|Stephen|Peter|Cornelius|Ananias|Sapphira)\b/i;

// Detects whether a query names a specific biblical narrative, figure, or story.
// Two complementary signals:
//   (a) LIKE-expanded topics include short proper-noun topics (1-3 words, capitalized).
//   (b) Raw query string contains known biblical proper nouns.
// Returns a NarrativeContext when both or either signal fires clearly, else undefined.
function detectQueryNarrative(
  query: string,
  _likeTopics: Array<{ id: number; name: string }>,
): NarrativeContext | undefined {
  // Signal (b): regex match on the raw query string.
  const regexMatch = BIBLICAL_PROPER_NOUN_PATTERN.exec(query);

  // Signal (a) is retained conceptually but not computed — the filter result
  // was never read since narrative mode requires signal (b) to fire.
  // Signal (a) alone is too prone to false positives from short theological
  // topic names in Nave's (e.g. FAITHFULNESS, GRACE OF GOD).

  // Determine the figure label from signal (b) if available.
  // Narrative mode requires signal (b) (the raw query regex) to fire;
  // signal (a) alone does NOT trigger narrative mode.
  let narrativeFigure: string | undefined;

  if (regexMatch) {
    narrativeFigure = regexMatch[1];
  }
  // Signal (a) alone does NOT trigger narrative mode — it only serves as
  // corroborating evidence when signal (b) already fired.

  if (!narrativeFigure) return undefined;

  return { narrativeMode: true, narrativeFigure };
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

// Returns a genre-appropriate, natural-language match reason for a major witness.
// Focuses on what this book contributes to the query rather than raw counts.
function buildWitnessMatchReason(
  candidate: WitnessCandidate,
  narrative?: string,
  themesMatched?: string[],
): string {
  const genre = BOOK_GENRE[candidate.book_name] ?? 'Unknown';
  const topThemes = themesMatched && themesMatched.length > 0
    ? themesMatched.slice(0, 3)
    : candidate.topic_names.split(',').map((n) => n.trim()).filter(Boolean).slice(0, 3);

  const themePhrase = topThemes.length > 1
    ? topThemes.slice(0, -1).join(', ') + ' and ' + topThemes[topThemes.length - 1]
    : topThemes[0] ?? 'this theme';

  if (narrative) {
    // Narrative arc within a book — focus on the story rather than counts.
    const chapterRange = `${candidate.book_name} ${candidate.min_chapter}–${candidate.max_chapter}`;
    switch (genre) {
      case 'Law':
        return `The ${narrative} account (${chapterRange}) grounds ${themePhrase} in covenant law and divine instruction.`;
      case 'History':
        return `The ${narrative} narrative (${chapterRange}) shows ${themePhrase} through a sustained historical story arc.`;
      case 'Gospel':
        return `The ${narrative} account (${chapterRange}) presents ${themePhrase} through the life and ministry of Jesus.`;
      default:
        return `The ${narrative} narrative (${chapterRange}) addresses ${themePhrase} through its story arc.`;
    }
  }

  switch (genre) {
    case 'Law':
      return `${candidate.book_name} establishes ${themePhrase} through foundational covenant law and divine ordinance.`;
    case 'History':
      return `${candidate.book_name} demonstrates ${themePhrase} through a sustained historical narrative.`;
    case 'Wisdom':
      return `${candidate.book_name} explores ${themePhrase} through wisdom reflection, dialogue, and instruction.`;
    case 'Poetry':
      return `${candidate.book_name} voices ${themePhrase} in prayer, lament, praise, and trust.`;
    case 'Prophecy':
      return `${candidate.book_name} develops ${themePhrase} through prophetic proclamation, judgment, and promise.`;
    case 'Gospel':
      return `${candidate.book_name} presents ${themePhrase} through the life, teaching, and ministry of Jesus.`;
    case 'Acts':
      return `${candidate.book_name} demonstrates ${themePhrase} through the early church's witness and mission.`;
    case 'Epistle':
      return `${candidate.book_name} teaches ${themePhrase} in doctrinal and pastoral terms.`;
    case 'Apocalyptic':
      return `${candidate.book_name} envisions ${themePhrase} in the context of ultimate divine victory and restoration.`;
    default:
      return `${candidate.book_name} addresses ${themePhrase} across ${candidate.chapter_count} chapters.`;
  }
}

// ─── Representative verse selection ──────────────────────────────────────────

async function fetchRepresentativeVerse(
  candidate: WitnessCandidate,
  matchedTopicIds: number[],
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
  narrativeContext?: NarrativeContext,
  seedChapterRange?: { minChapter: number; maxChapter: number },
): Promise<{ text: string; citation: Citation }> {
  // ── Narrative mode: primary narrative book ──────────────────────────────────
  // When the query names a specific narrative and this is the primary narrative
  // book, prefer a verse from within the densest chapter cluster of the story
  // unit. When a seed-only chapter range is available, use it instead of the
  // all-topic aggregation range — this constrains to e.g. Genesis 6-9 for Noah
  // rather than the full book span (Genesis 1-50).
  const isNarrativePrimaryBook =
    narrativeContext?.narrativeMode === true &&
    narrativeContext.primaryBookId === candidate.book_id;

  if (isNarrativePrimaryBook) {
    const kjvTranslation = getTranslation('KJV');
    const translationFilter = kjvTranslation
      ? `AND v.translation_id = ?`
      : `AND t.abbreviation = 'KJV'`;
    const translationParams: unknown[] = kjvTranslation ? [kjvTranslation.id] : [];

    // Use seed-only chapter range when available (tighter, story-specific span),
    // otherwise fall back to the all-topic aggregation range from the candidate.
    const chapterMin = seedChapterRange?.minChapter ?? candidate.min_chapter;
    const chapterMax = seedChapterRange?.maxChapter ?? candidate.max_chapter;

    // Tier 1 (narrative primary book): pick the verse with the highest topic
    // co-occurrence count within the seed chapter range. This selects a
    // thematically central verse (e.g. Genesis 7:7 or 8:1 for Noah) rather
    // than defaulting to verse 1 of the densest chapter (e.g. Genesis 9:1).
    if (matchedTopicIds.length > 0) {
      const topicPlaceholders = matchedTopicIds.map(() => '?').join(', ');
      const narrativeCooccurrenceResult = await d1.query(
        `SELECT
           ntv.chapter,
           ntv.verse,
           COUNT(DISTINCT ntv.topic_id) AS topic_count
         FROM nave_topic_verses ntv
         WHERE ntv.book_id = ?
           AND ntv.chapter >= ?
           AND ntv.chapter <= ?
           AND ntv.topic_id IN (${topicPlaceholders})
         GROUP BY ntv.chapter, ntv.verse
         ORDER BY topic_count DESC, ntv.chapter ASC, ntv.verse ASC
         LIMIT 1`,
        [candidate.book_id, chapterMin, chapterMax, ...matchedTopicIds],
      );

      if (narrativeCooccurrenceResult.results.length > 0) {
        const bestChapter = narrativeCooccurrenceResult.results[0]['chapter'] as number;
        const bestVerse = narrativeCooccurrenceResult.results[0]['verse'] as number;

        const cooccurrenceVerseResult = await d1.query(
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
             AND v.verse = ?
             ${translationFilter}
           LIMIT 1`,
          [candidate.book_id, bestChapter, bestVerse, ...translationParams],
        );

        if (cooccurrenceVerseResult.results.length > 0) {
          const r = cooccurrenceVerseResult.results[0] as unknown as VerseRow;
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
      }
    }

    // Fallback: find the densest chapter within the narrative span and pick verse 1.
    const topicFilter =
      matchedTopicIds.length > 0
        ? `AND ntv.topic_id IN (${matchedTopicIds.map(() => '?').join(', ')})`
        : '';
    const topicParams = matchedTopicIds.length > 0 ? matchedTopicIds : [];

    const narrativeChapterResult = await d1.query(
      `SELECT
         ntv.chapter,
         COUNT(*) AS topic_hits
       FROM nave_topic_verses ntv
       WHERE ntv.book_id = ?
         AND ntv.chapter >= ?
         AND ntv.chapter <= ?
         ${topicFilter}
       GROUP BY ntv.chapter
       ORDER BY topic_hits DESC
       LIMIT 1`,
      [candidate.book_id, chapterMin, chapterMax, ...topicParams],
    );

    const narrativeDenseChapter =
      narrativeChapterResult.results.length > 0
        ? (narrativeChapterResult.results[0]['chapter'] as number)
        : candidate.min_chapter;

    const narrativeVerseResult = await d1.query(
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
      [candidate.book_id, narrativeDenseChapter, ...translationParams],
    );

    if (narrativeVerseResult.results.length > 0) {
      const r = narrativeVerseResult.results[0] as unknown as VerseRow;
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
    // Fall through to standard selection if narrative query fails.
  }

  // Prefer verses that appear in nave_topic_verses for the matched topics AND
  // have a high Vectorize semantic score. This ensures the representative verse
  // is both topically grounded AND semantically close to the query.
  //
  // Build a set of topic-matched verse keys for this book so we can check
  // membership in O(1) during the Vectorize score scan.
  let topicMatchedKeys: Set<string> | null = null;

  if (matchedTopicIds.length > 0) {
    const placeholders = matchedTopicIds.map(() => '?').join(', ');
    const topicVerseResult = await d1.query(
      `SELECT DISTINCT chapter, verse
       FROM nave_topic_verses
       WHERE book_id = ?
         AND topic_id IN (${placeholders})`,
      [candidate.book_id, ...matchedTopicIds],
    );

    if (topicVerseResult.results.length > 0) {
      topicMatchedKeys = new Set<string>();
      for (const row of topicVerseResult.results) {
        topicMatchedKeys.add(`${candidate.book_id}:${row['chapter']}:${row['verse']}`);
      }
    }
  }

  // First pass: find the highest-scoring Vectorize hit that is also topic-matched.
  // Topic-matched verses receive a +0.15 bonus to their effective score so they
  // are preferred meaningfully over semantically similar but topically unmapped
  // verses. For broad theological queries (e.g. "God's faithfulness during
  // suffering"), Vectorize scores between candidate verses may differ by only
  // 0.05–0.10; a 0.15 bonus ensures the topic-grounded verse wins (e.g.
  // Psalm 89:1 over Psalm 9:14) without fully overriding genuinely superior
  // semantic matches (which typically differ by more than 0.15).
  const TOPIC_MATCH_BONUS = 0.15;
  // Within 0.01 of the best effective score, prefer the lower chapter:verse for stability.
  const SCORE_TIE_BAND = 0.01;

  let bestTopicEffectiveScore = -Infinity;
  let bestTopicScore = -Infinity;
  let bestTopicVerseRow: VerseRow | undefined;

  // Second pass fallback: any Vectorize hit in the book (if no topic-matched hits).
  let bestAnyEffectiveScore = -Infinity;
  let bestAnyScore = -Infinity;
  let bestAnyVerseRow: VerseRow | undefined;

  for (const [coordKey, coord] of semanticCoords) {
    if (coord.book_id !== candidate.book_id) continue;
    const verseRow = semanticVerseMap.get(coordKey);
    if (!verseRow) continue;

    const isTopicMatched = topicMatchedKeys !== null && topicMatchedKeys.has(coordKey);
    const effectiveScore = coord.score + (isTopicMatched ? TOPIC_MATCH_BONUS : 0);

    // Deterministic tiebreaker: within SCORE_TIE_BAND, prefer lower chapter then lower verse.
    const isAnyBetter =
      effectiveScore > bestAnyEffectiveScore + SCORE_TIE_BAND ||
      (Math.abs(effectiveScore - bestAnyEffectiveScore) <= SCORE_TIE_BAND &&
        bestAnyVerseRow !== undefined &&
        (verseRow.chapter < bestAnyVerseRow.chapter ||
          (verseRow.chapter === bestAnyVerseRow.chapter && verseRow.verse < bestAnyVerseRow.verse)));

    if (isAnyBetter) {
      bestAnyEffectiveScore = effectiveScore;
      bestAnyScore = coord.score;
      bestAnyVerseRow = verseRow;
    }

    if (isTopicMatched) {
      const isTopicBetter =
        effectiveScore > bestTopicEffectiveScore + SCORE_TIE_BAND ||
        (Math.abs(effectiveScore - bestTopicEffectiveScore) <= SCORE_TIE_BAND &&
          bestTopicVerseRow !== undefined &&
          (verseRow.chapter < bestTopicVerseRow.chapter ||
            (verseRow.chapter === bestTopicVerseRow.chapter && verseRow.verse < bestTopicVerseRow.verse)));

      if (isTopicBetter) {
        bestTopicEffectiveScore = effectiveScore;
        bestTopicScore = coord.score;
        bestTopicVerseRow = verseRow;
      }
    }
  }

  // Suppress unused variable warning — bestTopicScore and bestAnyScore are intentionally
  // retained for potential future debug logging without affecting runtime behavior.
  void bestTopicScore;
  void bestAnyScore;

  // Prefer topic-matched + semantic verse, fall back to any semantic verse.
  const chosen = bestTopicVerseRow ?? bestAnyVerseRow;
  if (chosen) {
    return {
      text: chosen.text,
      citation: {
        book: chosen.book_name,
        chapter: chosen.chapter,
        verse: chosen.verse,
        translation: chosen.translation_abbrev,
      },
    };
  }

  // Last-resort fallback: no Vectorize hits exist for this book.
  // Instead of defaulting to verse 1 of the densest chapter (which produces
  // poor anchors like Isaiah 1:1), find the verse that appears in the most
  // matched topics within this book — the highest co-occurrence verse.
  const kjvTranslation = getTranslation('KJV');
  const translationFilter = kjvTranslation
    ? `AND v.translation_id = ?`
    : `AND t.abbreviation = 'KJV'`;
  const translationParams: unknown[] = kjvTranslation ? [kjvTranslation.id] : [];

  if (matchedTopicIds.length > 0) {
    // Step 1: find the verse with the highest co-occurrence count across matched topics.
    const topicPlaceholders = matchedTopicIds.map(() => '?').join(', ');
    const cooccurrenceResult = await d1.query(
      `SELECT
         ntv.chapter,
         ntv.verse,
         COUNT(DISTINCT ntv.topic_id) AS topic_count
       FROM nave_topic_verses ntv
       WHERE ntv.book_id = ?
         AND ntv.topic_id IN (${topicPlaceholders})
       GROUP BY ntv.chapter, ntv.verse
       ORDER BY topic_count DESC, ntv.chapter ASC, ntv.verse ASC
       LIMIT 1`,
      [candidate.book_id, ...matchedTopicIds],
    );

    if (cooccurrenceResult.results.length > 0) {
      const bestChapter = cooccurrenceResult.results[0]['chapter'] as number;
      const bestVerse = cooccurrenceResult.results[0]['verse'] as number;

      const cooccurrenceVerseResult = await d1.query(
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
           AND v.verse = ?
           ${translationFilter}
         LIMIT 1`,
        [candidate.book_id, bestChapter, bestVerse, ...translationParams],
      );

      if (cooccurrenceVerseResult.results.length > 0) {
        const r = cooccurrenceVerseResult.results[0] as unknown as VerseRow;
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
    }

    // Step 2: fall back to the densest chapter, then pick the lowest verse number
    // among topic-matched verses in that chapter (not just verse 1).
    const denseChapterResult = await d1.query(
      `SELECT
         ntv.chapter,
         COUNT(*) AS topic_hits
       FROM nave_topic_verses ntv
       WHERE ntv.book_id = ?
         AND ntv.topic_id IN (${topicPlaceholders})
       GROUP BY ntv.chapter
       ORDER BY topic_hits DESC
       LIMIT 1`,
      [candidate.book_id, ...matchedTopicIds],
    );

    if (denseChapterResult.results.length > 0) {
      const denseChapter = denseChapterResult.results[0]['chapter'] as number;

      // Pick the lowest verse number among topic-matched verses in this chapter.
      const minTopicVerseResult = await d1.query(
        `SELECT MIN(ntv.verse) AS min_verse
         FROM nave_topic_verses ntv
         WHERE ntv.book_id = ?
           AND ntv.chapter = ?
           AND ntv.topic_id IN (${topicPlaceholders})`,
        [candidate.book_id, denseChapter, ...matchedTopicIds],
      );

      const targetVerse =
        minTopicVerseResult.results.length > 0 &&
        minTopicVerseResult.results[0]['min_verse'] != null
          ? (minTopicVerseResult.results[0]['min_verse'] as number)
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
           AND v.verse = ?
           ${translationFilter}
         LIMIT 1`,
        [candidate.book_id, denseChapter, targetVerse, ...translationParams],
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
    }
  }

  // Absolute last resort: return empty placeholder (should never happen in practice).
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

// Builds a genre-aware, natural-language explanation of why this book is a
// principal witness for the query. Answers: "Why does this book matter for
// THIS query?" rather than reporting analytics.
//
// The explanation selects a genre-appropriate template and weaves in the
// top matched themes. Verse count and coverage are available as supporting
// context but are not the lead.
function buildWhyThisBookMatters(
  candidate: WitnessCandidate,
  salienceMap: Map<string, number>,
  salienceTopicIds: number[],
  expandedTopics: Array<{ id: number; name: string }>,
  precomputedThemes?: string[],
): string {
  const genre = BOOK_GENRE[candidate.book_name] ?? 'Unknown';

  // Resolve the top themes: prefer pre-computed themes (already salience-sorted),
  // otherwise derive from salience data, otherwise fall back to candidate topics.
  let topThemes: string[];

  if (precomputedThemes && precomputedThemes.length > 0) {
    topThemes = precomputedThemes.slice(0, 3);
  } else {
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
    topThemes = scored.slice(0, 3).map((t) => t.name);
  }

  // If still empty, fall back to the candidate's own topic list.
  if (topThemes.length === 0) {
    topThemes = candidate.topic_names
      .split(',')
      .map((n) => n.trim())
      .filter(Boolean)
      .slice(0, 3);
  }

  // Format theme list naturally: "A, B and C" or "A and B" or just "A".
  const themePhrase =
    topThemes.length > 1
      ? topThemes.slice(0, -1).join(', ') + ' and ' + topThemes[topThemes.length - 1]
      : topThemes[0] ?? 'this theme';

  // Select a genre-appropriate explanation template.
  switch (genre) {
    case 'Law':
      return `${candidate.book_name} is one of the Bible's foundational treatments of ${themePhrase}, grounding it in covenant law, divine instruction, and Israel's formative story.`;
    case 'History':
      return `${candidate.book_name} is one of the Bible's central sustained treatments of ${themePhrase}, showing its significance through a sustained historical narrative.`;
    case 'Wisdom':
      return `${candidate.book_name} is one of the Bible's central sustained treatments of ${themePhrase}, exploring it through wisdom reflection, dialogue, and instruction.`;
    case 'Poetry':
      return `${candidate.book_name} repeatedly voices ${themePhrase} in prayer, lament, praise, and trust, making it a primary devotional witness to this theme.`;
    case 'Prophecy':
      return `${candidate.book_name} develops ${themePhrase} through prophetic proclamation, judgment, comfort, and restoration, offering one of Scripture's richest prophetic treatments.`;
    case 'Gospel':
      return `${candidate.book_name} presents ${themePhrase} through the life, teaching, and ministry of Jesus, grounding the theme in the person and work of Christ.`;
    case 'Acts':
      return `${candidate.book_name} demonstrates ${themePhrase} in the life of the early church, showing how the apostolic community embodied and proclaimed this theme.`;
    case 'Epistle':
      return `${candidate.book_name} explicitly teaches ${themePhrase} in doctrinal and pastoral terms, offering some of the New Testament's clearest theological exposition of this theme.`;
    case 'Apocalyptic':
      return `${candidate.book_name} envisions ${themePhrase} in the context of ultimate divine victory and cosmic restoration, providing the Bible's most sustained apocalyptic treatment.`;
    default:
      return `${candidate.book_name} is a principal biblical witness to ${themePhrase} across ${candidate.chapter_count} chapters.`;
  }
}

// Maps raw Nave's taxonomy topic names to user-facing thematic labels.
// Keys are the exact uppercase names used in the database.
// Values are clean, human-readable labels shown in the output.
const TOPIC_LABEL_MAP: Record<string, string> = {
  'AFFLICTIONS AND ADVERSITIES': 'affliction',
  'AFFLICTIONS': 'affliction',
  'ADVERSITY': 'adversity',
  'ANGER': 'anger',
  'ANXIETY': 'anxiety',
  'APOSTASY': 'apostasy',
  'ATONEMENT': 'atonement',
  'BAPTISM': 'baptism',
  'BLESSINGS': 'blessing',
  'CALLING': 'calling',
  'CHARITY': 'generosity',
  'CHILDREN': 'children',
  'CHILDREN OF ISRAEL': 'Israel',
  'CHRIST': 'Christ',
  'CHURCH': 'the church',
  'COMFORT': 'comfort',
  'COMMANDMENTS': 'commandments',
  'COMPASSION': 'compassion',
  'CONSCIENCE': 'conscience',
  'CONTENTMENT': 'contentment',
  'COURAGE': 'courage',
  'COVENANT': 'covenant',
  'COVETOUSNESS': 'covetousness',
  'CREATION': 'creation',
  'DEATH': 'death',
  'DELIVERANCE': 'deliverance',
  'DISCIPLESHIP': 'discipleship',
  'DISCIPLINE': 'discipline',
  'DIVINE GUIDANCE': 'guidance',
  'DIVINE PROTECTION': 'protection',
  'DIVINE SOVEREIGNTY': 'sovereignty',
  'DOUBT': 'doubt',
  'ELECTION': 'election',
  'ENDURANCE': 'endurance',
  'ETERNAL LIFE': 'eternal life',
  'EVANGELISM': 'evangelism',
  'EVIL': 'evil',
  'EXILE': 'exile',
  'FAITH': 'faith',
  'FAITHFULNESS': 'faithfulness',
  'FAMILY': 'family',
  'FASTING': 'fasting',
  'FEAR': 'fear',
  'FEAR OF GOD': 'fear of God',
  'FORGIVENESS': 'forgiveness',
  'FRIENDSHIP': 'friendship',
  'GENEROSITY': 'generosity',
  'GLORIFICATION': 'glorification',
  'GLORY': 'glory',
  'GOD': 'God',
  'GRACE': 'grace',
  'GRATITUDE': 'gratitude',
  'GRIEF': 'grief',
  'GUIDANCE': 'guidance',
  'HEALING': 'healing',
  'HOLINESS': 'holiness',
  'HOLY SPIRIT': 'the Holy Spirit',
  'HOPE': 'hope',
  'HUMILITY': 'humility',
  'IDOLATRY': 'idolatry',
  'ISRAEL': 'Israel',
  'JESUS CHRIST': 'Jesus Christ',
  'JOY': 'joy',
  'JUDGMENT': 'judgment',
  'JUSTICE': 'justice',
  'KINGDOM OF GOD': 'the kingdom of God',
  'LAW': 'the law',
  'LAMENT': 'lament',
  'LEADERSHIP': 'leadership',
  'LIGHT': 'light',
  'LONELINESS': 'loneliness',
  'LORD': 'the Lord',
  'LOVE': 'love',
  'LOVE OF GOD': 'God\'s love',
  'MARRIAGE': 'marriage',
  'MERCY': 'mercy',
  'MISSIONS': 'missions',
  'OBEDIENCE': 'obedience',
  'OMNIPOTENCE': 'God\'s power',
  'PATIENCE': 'patience',
  'PEACE': 'peace',
  'PERSEVERANCE': 'perseverance',
  'PRAISE': 'praise',
  'PRAYER': 'prayer',
  'PRIDE': 'pride',
  'PROPHECY': 'prophecy',
  'PROVIDENCE': 'providence',
  'PURIFICATION': 'purification',
  'REDEMPTION': 'redemption',
  'RENEWAL': 'renewal',
  'REPENTANCE': 'repentance',
  'RESTORATION': 'restoration',
  'RESURRECTION': 'resurrection',
  'REVELATION': 'revelation',
  'RIGHTEOUSNESS': 'righteousness',
  'SACRIFICE': 'sacrifice',
  'SALVATION': 'salvation',
  'SANCTIFICATION': 'sanctification',
  'SERVANT': 'servanthood',
  'SERVICE': 'service',
  'SIN': 'sin',
  'SOVEREIGNTY OF GOD': 'God\'s sovereignty',
  'STRENGTH': 'strength',
  'SUFFERING': 'suffering',
  'TEMPTATION': 'temptation',
  'THANKSGIVING': 'thanksgiving',
  'TRIALS': 'trials',
  'TRUST': 'trust',
  'TRUTH': 'truth',
  'UNFAITHFULNESS': 'unfaithfulness',
  'UNITY': 'unity',
  'VICTORY': 'victory',
  'WISDOM': 'wisdom',
  'WORKS': 'works',
  'WORSHIP': 'worship',
  'WRATH': 'wrath',
  'WRATH OF GOD': 'God\'s wrath',
};

// Transforms a raw Nave's topic name into a user-facing thematic label.
// Checks the explicit map first; falls back to a normalized transform:
//   1. Lowercase the raw name
//   2. Handle "X OF Y" genitive patterns → possessive or compact form
//      e.g. "CHILDREN OF ISRAEL" → "Israel's children", "LOVE OF GOD" → "God's love"
//   3. Strip leading articles ("the ", "a ", "an ")
//   4. Remove " and " conjunctions that create verbose compound labels
//      (e.g. "afflictions and adversities" → "affliction")
//      only when it would shorten to a single recognizable word
//   5. Title-case proper nouns that appear after "of" (names, God)
function toThemeLabel(topicName: string): string {
  const mapped = TOPIC_LABEL_MAP[topicName];
  if (mapped !== undefined) return mapped;

  let label = topicName.toLowerCase();

  // Handle "X OF Y" genitive patterns for proper nouns.
  // Match patterns like "love of god", "children of israel", "grace of christ".
  const ofMatch = label.match(/^(.+?) of (god|christ|jesus|israel|the lord|the holy spirit)$/);
  if (ofMatch) {
    const subject = ofMatch[1].trim();
    const owner = ofMatch[2].replace(/^the /, '');
    // Title-case the owner name.
    const ownerLabel = owner.charAt(0).toUpperCase() + owner.slice(1);
    return `${ownerLabel}'s ${subject}`;
  }

  // Strip leading articles: "the ", "a ", "an ".
  label = label.replace(/^(the |a |an )/, '');

  // Collapse " and " conjunctions in compound topic names:
  // keep the first part when the compound is just two near-synonyms.
  // e.g. "afflictions and adversities" → "affliction" (already mapped, but as fallback)
  // Only collapse when the resulting first word is >= 5 chars (avoids collapsing "sin and grace").
  const andParts = label.split(' and ');
  if (andParts.length === 2) {
    const first = andParts[0].trim();
    const second = andParts[1].trim();
    // If both parts share a root (first 5 chars), keep just the shorter one.
    if (first.length >= 5 && second.length >= 5 && first.slice(0, 5) === second.slice(0, 5)) {
      label = first.length <= second.length ? first : second;
    }
  }

  return label;
}

// Minimum salience score for a topic to appear in themes_matched output.
// Topics below this threshold are considered too loosely related to the query
// to surface as user-facing theme labels.
const THEMES_MATCHED_SALIENCE_THRESHOLD = 0.6;
// Stricter salience threshold applied to co-occurring (non-seed) topics.
// Seed topics are the direct query matches; co-occurring topics are discovered
// via topic expansion. The higher threshold prevents noisy labels like
// "the church" from appearing on unrelated queries (e.g. faithfulness).
const THEMES_MATCHED_SALIENCE_THRESHOLD_COOCCURRING = 0.85;
// Minimum semantic similarity score a co-occurring (non-seed) topic must have
// to appear in themes_matched output. Topics matched only via LIKE expansion
// are exempt (LIKE match is treated as fully relevant). Semantic topics that
// scored below this threshold are considered tangentially related — e.g.
// "the church" appearing for a suffering query because it co-occurs with
// AFFLICTIONS in Nave's, not because it is semantically close to the query.
const THEMES_MATCHED_SEMANTIC_SCORE_THRESHOLD_COOCCURRING = 0.75;
// Maximum number of theme labels to include in themes_matched.
const THEMES_MATCHED_MAX = 5;

// Builds the list of query-relevant themes for a witness book.
// Filters topic_names to only those topics that appear in expandedTopics
// (the query-matched topic set), with salience >= 0.6 for this book, capped
// at top 5 by salience. Internal scoring uses original topic names; output
// labels are user-facing.
// seedTopicIdSet distinguishes direct query matches (seed) from co-occurring
// expansions. Seed topics use the standard 0.6 threshold; co-occurring topics
// use a stricter 0.85 threshold to avoid noisy labels.
// topicSemanticScoreById provides Vectorize similarity scores for topics that
// were matched semantically (not via LIKE). Co-occurring topics that are not
// LIKE-matched must also meet a minimum semantic score to appear — this filters
// tangentially related topics like "the church" from a suffering query.
// likeTopicIdSet identifies LIKE-matched topics, which are exempt from the
// semantic score filter (LIKE is already keyword-grounded).
function buildThemesMatched(
  candidate: WitnessCandidate,
  expandedTopics: Array<{ id: number; name: string }>,
  salienceMap: Map<string, number>,
  salienceTopicIds: number[],
  seedTopicIdSet?: Set<number>,
  topicSemanticScoreById?: Map<number, number>,
  likeTopicIdSet?: Set<number>,
): string[] {
  const expandedNameSet = new Set(expandedTopics.map((t) => t.name));

  const candidateTopics = candidate.topic_names
    .split(',')
    .map((n) => n.trim())
    .filter(Boolean);

  // Keep only topics that are in the query-matched expanded topic set.
  const matched = candidateTopics.filter((name) => expandedNameSet.has(name));

  // Build a salience score lookup by topic name for this book.
  // Scoring uses original topic names (not labels) to preserve correctness.
  const topicIdByName = new Map(expandedTopics.map((t) => [t.name, t.id]));
  const getSalience = (name: string): number => {
    const topicId = topicIdByName.get(name);
    if (!topicId || !salienceTopicIds.includes(topicId)) return 0;
    return salienceMap.get(`${candidate.book_id}:${topicId}`) ?? 0;
  };

  // Returns the effective salience threshold for a topic name.
  // Co-occurring (non-seed) topics require a higher salience to qualify
  // to prevent noisy labels (e.g. "the church" on faithfulness queries).
  const getThreshold = (name: string): number => {
    if (!seedTopicIdSet) return THEMES_MATCHED_SALIENCE_THRESHOLD;
    const topicId = topicIdByName.get(name);
    if (!topicId) return THEMES_MATCHED_SALIENCE_THRESHOLD;
    return seedTopicIdSet.has(topicId)
      ? THEMES_MATCHED_SALIENCE_THRESHOLD
      : THEMES_MATCHED_SALIENCE_THRESHOLD_COOCCURRING;
  };

  // Returns true if a co-occurring (non-seed) topic passes the semantic
  // relevance check. LIKE-matched topics are always considered relevant.
  // Semantic-only topics must have a Vectorize similarity score meeting
  // THEMES_MATCHED_SEMANTIC_SCORE_THRESHOLD_COOCCURRING. Seed topics are
  // exempt — they are already primary query matches.
  const passesSemanticCheck = (name: string): boolean => {
    if (!seedTopicIdSet || !topicSemanticScoreById) return true;
    const topicId = topicIdByName.get(name);
    if (!topicId) return true;
    // Seed topics are always accepted.
    if (seedTopicIdSet.has(topicId)) return true;
    // LIKE-matched topics are keyword-grounded — exempt from semantic filter.
    if (likeTopicIdSet && likeTopicIdSet.has(topicId)) return true;
    // Semantic-only co-occurring topic: require minimum similarity score.
    const semScore = topicSemanticScoreById.get(topicId) ?? 0;
    return semScore >= THEMES_MATCHED_SEMANTIC_SCORE_THRESHOLD_COOCCURRING;
  };

  if (matched.length > 0) {
    // Filter to topics whose salience for this book exceeds the threshold
    // AND that pass the semantic relevance check for co-occurring topics.
    // Sort by salience descending, and cap at THEMES_MATCHED_MAX.
    const salienceFiltered = matched.filter(
      (name) => getSalience(name) >= getThreshold(name) && passesSemanticCheck(name),
    );

    if (salienceFiltered.length > 0) {
      salienceFiltered.sort((a, b) => getSalience(b) - getSalience(a));
      return salienceFiltered.slice(0, THEMES_MATCHED_MAX).map(toThemeLabel);
    }

    // Salience threshold fallback: when no topics pass the strict thresholds
    // for this book, fall back to seed-only topics by raw salience score. This
    // ensures the themes_matched list is never empty for a book that does have
    // seed-matched topics — the thresholds are quality filters, not hard gates.
    const seedOnly = matched.filter((name) => {
      if (!seedTopicIdSet) return true;
      const topicId = topicIdByName.get(name);
      return topicId ? seedTopicIdSet.has(topicId) : true;
    });
    const fallbackPool = seedOnly.length > 0 ? seedOnly : matched;
    fallbackPool.sort((a, b) => getSalience(b) - getSalience(a));
    return fallbackPool.slice(0, THEMES_MATCHED_MAX).map(toThemeLabel);
  }

  // No query-matched topics found: return first 5 candidate topics as labels.
  return candidateTopics.slice(0, THEMES_MATCHED_MAX).map(toThemeLabel);
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

// Keywords that indicate a comfort/hope/faithfulness theme in the query.
// Used by Prophecy clustering to bias toward consolation sections.
const CONSOLATION_KEYWORDS = [
  'comfort', 'hope', 'faithful', 'faithfulness', 'promise', 'restore',
  'restoration', 'consolation', 'peace', 'salvation', 'redemption',
  'deliverance', 'mercy', 'grace', 'blessing', 'renewal', 'trust',
];

// Returns true when the query topic semantically relates to comfort or hope.
function isConsolationQuery(queryTopic: string): boolean {
  const lower = queryTopic.toLowerCase();
  return CONSOLATION_KEYWORDS.some((kw) => lower.includes(kw));
}

// Formats a single AnchorChapterRow into a passage range string.
function formatPassageRange(
  bookName: string,
  start: number,
  end: number,
  minVerse: number,
  maxVerse: number,
): string {
  if (start === end) {
    const verseRange = maxVerse - minVerse;
    if (verseRange < 10 && minVerse !== maxVerse) {
      return `${bookName} ${start}:${minVerse}-${maxVerse}`;
    }
    return `${bookName} ${start}`;
  }
  return `${bookName} ${start}-${end}`;
}

// Clusters chapter rows into consecutive spans (gap <= maxGap).
function buildSpans(
  chapters: AnchorChapterRow[],
  maxGap: number,
): Array<{ start: number; end: number; totalHits: number; minVerse: number; maxVerse: number }> {
  const spans: Array<{ start: number; end: number; totalHits: number; minVerse: number; maxVerse: number }> = [];
  let currentSpan: typeof spans[0] | null = null;

  for (const row of chapters) {
    if (!currentSpan) {
      currentSpan = { start: row.chapter, end: row.chapter, totalHits: row.hit_count, minVerse: row.min_verse, maxVerse: row.max_verse };
    } else if (row.chapter - currentSpan.end <= maxGap) {
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

  return spans;
}

// ─── Genre-aware passage clustering strategies ────────────────────────────────
//
// Poetry (Psalms, Lamentations): Prefer individual thematically dense chapters
// rather than consecutive ranges. Psalms are independent poems — consecutive
// clustering artificially groups unrelated psalms. Return top 3 chapters by
// hit count as individual references.
//
// Narrative (Law, History, Gospel, Acts): Bias toward the arc structure of the
// book — beginning, middle, and end — to reflect entry, crisis, and resolution
// rather than just the single densest cluster.
//
// Prophecy: When the query relates to comfort/hope/faithfulness, bias toward
// consolation sections in the latter half of the book. Otherwise use density.
//
// Epistle and default: Density-based consecutive clustering (unchanged).

function clusterPoetry(
  rows: AnchorChapterRow[],
  bookName: string,
): string[] {
  if (rows.length === 0) return [];
  // Return up to 3 individual dense chapters — no consecutive merging.
  const sorted = [...rows].sort((a, b) => b.hit_count - a.hit_count);
  const top = sorted.slice(0, 3);
  // Sort by chapter number for canonical order in output.
  top.sort((a, b) => a.chapter - b.chapter);
  return top.map((r) => {
    const verseRange = r.max_verse - r.min_verse;
    if (verseRange < 10 && r.min_verse !== r.max_verse) {
      return `${bookName} ${r.chapter}:${r.min_verse}-${r.max_verse}`;
    }
    return `${bookName} ${r.chapter}`;
  });
}

function clusterNarrative(
  rows: AnchorChapterRow[],
  bookName: string,
  totalBookChapters: number,
): string[] {
  if (rows.length === 0) return [];

  // Small seed span: when rows span ≤6 chapters (already pre-filtered to the
  // story unit for the primary narrative book), bypass arc-based thirds logic
  // and return the full contiguous seed chapter range as a single passage.
  // This prevents Genesis 6-9 from being fragmented — arc thirds of Genesis
  // (50 chapters) would put chapters 6-9 entirely in the first arc, returning
  // only the single densest chapter (e.g. chapter 9) rather than the full span.
  const seedMinChapter = Math.min(...rows.map((r) => r.chapter));
  const seedMaxChapter = Math.max(...rows.map((r) => r.chapter));
  const seedSpan = seedMaxChapter - seedMinChapter + 1;
  if (seedSpan <= 6) {
    return [`${bookName} ${seedMinChapter}-${seedMaxChapter}`];
  }

  // Divide the book into thirds: entry arc, crisis arc, resolution arc.
  const arcSize = Math.ceil(totalBookChapters / 3);
  const entryEnd = arcSize;
  const crisisEnd = arcSize * 2;

  const entryRows = rows.filter((r) => r.chapter <= entryEnd);
  const crisisRows = rows.filter((r) => r.chapter > entryEnd && r.chapter <= crisisEnd);
  const resolutionRows = rows.filter((r) => r.chapter > crisisEnd);

  // For each arc, find the single densest chapter.
  const arcRepresentatives: AnchorChapterRow[] = [];
  for (const arcRows of [entryRows, crisisRows, resolutionRows]) {
    if (arcRows.length === 0) continue;
    const densest = arcRows.reduce((best, r) => r.hit_count > best.hit_count ? r : best);
    arcRepresentatives.push(densest);
  }

  if (arcRepresentatives.length === 0) return [];

  // Sort by chapter for canonical order, then build spans (gap <= 3 to preserve arc grouping).
  arcRepresentatives.sort((a, b) => a.chapter - b.chapter);
  const spans = buildSpans(arcRepresentatives, 3);
  spans.sort((a, b) => b.totalHits - a.totalHits);

  return spans.slice(0, 3).map((span) =>
    formatPassageRange(bookName, span.start, span.end, span.minVerse, span.maxVerse)
  );
}

function clusterProphecy(
  rows: AnchorChapterRow[],
  bookName: string,
  totalBookChapters: number,
  queryTopic: string,
): string[] {
  if (rows.length === 0) return [];

  // For comfort/hope/faithfulness queries, prefer the latter half of prophetic
  // books where consolation oracles typically appear (e.g. Isaiah 40-66).
  const consolation = isConsolationQuery(queryTopic);
  let candidates = rows;

  if (consolation && totalBookChapters >= 10) {
    const midpoint = Math.ceil(totalBookChapters / 2);
    const consolationRows = rows.filter((r) => r.chapter > midpoint);
    // Only use consolation filter if it yields any results.
    if (consolationRows.length > 0) {
      candidates = consolationRows;
    }
  }

  // Within selected candidates, use density-based consecutive clustering.
  const MIN_HIT_THRESHOLD = 2;
  let qualifying = candidates.filter((r) => r.hit_count >= MIN_HIT_THRESHOLD);
  if (qualifying.length === 0) {
    qualifying = [...candidates].sort((a, b) => b.hit_count - a.hit_count).slice(0, 3);
  }

  qualifying.sort((a, b) => b.hit_count - a.hit_count);
  const topChapters = qualifying.slice(0, 12);
  topChapters.sort((a, b) => a.chapter - b.chapter);

  const spans = buildSpans(topChapters, 2);
  spans.sort((a, b) => b.totalHits - a.totalHits);

  return spans.slice(0, 3).map((span) =>
    formatPassageRange(bookName, span.start, span.end, span.minVerse, span.maxVerse)
  );
}

// Clusters chapter rows for a single book into passage range strings using a
// genre-aware strategy. Returns at most 3 passage ranges.
//
// Genre strategies:
//   Poetry     — Individual dense chapters (no consecutive merging)
//   Narrative  — Arc-based (entry / crisis / resolution thirds of the book)
//   Prophecy   — Consolation-biased for comfort/hope queries; density otherwise
//   Epistle    — Density-based consecutive clustering (unchanged)
//   Default    — Density-based consecutive clustering
function clusterToPassageRanges(
  rows: AnchorChapterRow[],
  bookName: string,
  totalBookChapters: number,
  genre?: string,
  queryTopic?: string,
): string[] {
  if (rows.length === 0) return [];

  // Short book: if coverage spans >= 80% of the book, just return the book name.
  const minChapter = Math.min(...rows.map((r) => r.chapter));
  const maxChapter = Math.max(...rows.map((r) => r.chapter));
  const spanCount = maxChapter - minChapter + 1;
  if (spanCount >= totalBookChapters * 0.8 && totalBookChapters <= 5) {
    return [bookName];
  }

  // Poetry: individual dense chapters (Psalms, Lamentations).
  if (genre === 'Poetry') {
    return clusterPoetry(rows, bookName);
  }

  // Narrative: arc-based clustering (Law, History, Gospel, Acts).
  if (
    genre === 'Law' ||
    genre === 'History' ||
    genre === 'Gospel' ||
    genre === 'Acts'
  ) {
    return clusterNarrative(rows, bookName, totalBookChapters);
  }

  // Prophecy: consolation-biased for comfort/hope queries.
  if (genre === 'Prophecy') {
    return clusterProphecy(rows, bookName, totalBookChapters, queryTopic ?? '');
  }

  // Epistle, Wisdom, Apocalyptic, and default: density-based consecutive clustering.
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

  const spans = buildSpans(topChapters, 2);

  // Sort spans by aggregate hit count descending.
  spans.sort((a, b) => b.totalHits - a.totalHits);

  // Emit up to 3 passage range strings.
  return spans.slice(0, 3).map((span) =>
    formatPassageRange(bookName, span.start, span.end, span.minVerse, span.maxVerse)
  );
}

// Builds a genre-aware narrative_reason string when detectNarrative returns a result.
// Emphasizes what kind of witness the narrative arc provides for the query.
// Returns undefined for non-narrative books.
function buildNarrativeReason(
  narrative: string | undefined,
  candidate: WitnessCandidate,
  themesMatched?: string[],
): string | undefined {
  if (!narrative) return undefined;

  const genre = BOOK_GENRE[candidate.book_name] ?? 'Unknown';
  const chapterRange = `${candidate.book_name} ${candidate.min_chapter}–${candidate.max_chapter}`;

  const topThemes = themesMatched && themesMatched.length > 0
    ? themesMatched.slice(0, 2)
    : [];
  const themeClause = topThemes.length > 0
    ? ` — particularly ${topThemes.join(' and ')}`
    : '';

  switch (genre) {
    case 'Law':
      return `The ${narrative} account (${chapterRange}) anchors the theme in covenant narrative and divine instruction${themeClause}, rather than abstract teaching.`;
    case 'History':
      return `The ${narrative} account (${chapterRange}) traces this theme through a sustained historical narrative${themeClause}, showing how it played out in Israel's story.`;
    case 'Gospel':
      return `The ${narrative} account (${chapterRange}) shows this theme in the life and ministry of Jesus${themeClause}, grounding it in the Gospel story.`;
    default:
      // Law-adjacent and other narrative sections default to a clear, readable form.
      return `The ${narrative} narrative (${chapterRange}) engages this theme through story${themeClause} rather than systematic exposition.`;
  }
}

// ─── Query-alignment scoring ──────────────────────────────────────────────────

// Computes a [0, 1] query-alignment score for a book, representing how well the
// book's evidence (matched topics + verse-level semantic hits) aligns with the
// user's specific query — as opposed to the general topic domain.
//
// Inputs:
//   candidateBookId      — the book being scored
//   matchedTopicIds      — topic IDs that caused this book to appear
//   topicRelevanceWeight — per-topic query-relevance weight (1.0 for LIKE, semantic score for Vectorize)
//   semanticCoords       — verse-level Vectorize hit map (book_id:chapter:verse → score)
//   semanticHitsPerBook  — count of verse-level semantic hits per book
//   maxSemanticHits      — the highest per-book semantic hit count across all candidates
//
// The score combines two normalized signals:
//   A) Topic query-alignment: average relevance weight of the book's matched topics.
//      Topics with weight close to 1.0 were retrieved by exact keyword match
//      (directly named in the query). Semantic-only topics have lower weights,
//      reducing noise from loosely related categories.
//   B) Semantic verse density: fraction of this book's verse hits relative to the
//      book with the most hits. A book that absorbs many of the top Vectorize verse
//      results is strongly aligned with the query's actual embedding.
//
// Final score = 0.5 * A + 0.5 * B  (equal-weight blend of both signals)
function computeQueryAlignmentScore(
  candidateBookId: number,
  matchedTopicIds: number[],
  topicRelevanceWeight: Map<number, number>,
  semanticHitsPerBook: Map<number, number>,
  maxSemanticHits: number,
): number {
  // Signal A: average query-relevance weight of matched topics.
  let topicAlignmentScore = 0;
  if (matchedTopicIds.length > 0) {
    let weightSum = 0;
    for (const tid of matchedTopicIds) {
      weightSum += topicRelevanceWeight.get(tid) ?? 0.5;
    }
    topicAlignmentScore = weightSum / matchedTopicIds.length;
  }

  // Signal B: normalized semantic verse density.
  const hits = semanticHitsPerBook.get(candidateBookId) ?? 0;
  const semanticDensityScore = maxSemanticHits > 0 ? hits / maxSemanticHits : 0;

  return 0.5 * topicAlignmentScore + 0.5 * semanticDensityScore;
}

// Builds a concise human-readable note explaining why this specific book's
// evidence aligns with the user's actual query, connecting query terms to
// the matched themes and semantic signals.
//
// This is distinct from match_reason (which describes genre-appropriate
// contribution) and why_this_book_matters (which explains the book's broad
// significance). This note is query-specific: it names the actual query topic
// and the top matched themes, making clear the evidence is tailored to the
// exact search rather than a generic topic association.
function buildQueryAlignmentNote(
  bookName: string,
  queryTopic: string,
  themesMatched: string[],
  semanticHits: number,
  queryAlignmentScore: number,
): string {
  const topThemes = themesMatched.slice(0, 2);
  const themePhrase =
    topThemes.length > 1
      ? topThemes[0] + ' and ' + topThemes[1]
      : topThemes[0] ?? queryTopic;

  // Choose phrasing based on how well both signals fired.
  const hasStrongSemanticSignal = semanticHits >= 2;
  const hasStrongTopicAlignment = queryAlignmentScore >= 0.6;

  if (hasStrongSemanticSignal && hasStrongTopicAlignment) {
    return (
      `${bookName}'s treatment of ${themePhrase} aligns closely with "${queryTopic}" ` +
      `— supported by both curated topic matches and direct semantic verse similarity.`
    );
  }

  if (hasStrongSemanticSignal) {
    return (
      `Multiple verses in ${bookName} are semantically close to "${queryTopic}", ` +
      `indicating strong thematic resonance with the query beyond keyword matching.`
    );
  }

  if (hasStrongTopicAlignment) {
    return (
      `${bookName}'s curated topics (${themePhrase}) are directly named in or closely ` +
      `related to "${queryTopic}", making it a topically grounded witness.`
    );
  }

  // Weak signal — acknowledge the association is present but indirect.
  return (
    `${bookName} addresses themes related to "${queryTopic}" (${themePhrase}), ` +
    `though the connection is drawn from broader topical associations rather than direct keyword or semantic matches.`
  );
}

// ─── Polysemous narrative topic names ────────────────────────────────────────

// Topics whose Nave's coverage spans multiple unrelated narratives or contexts.
// Excluded from the seed-only aggregation in narrative mode to prevent false
// seed-verse counts in books that reference the topic in a different story context.
// Example: ARK covers both Noah's ark (Genesis) and the Ark of the Covenant
// (Exodus/Numbers); including it inflates seed counts for Exodus on a Noah query.
const POLYSEMOUS_NARRATIVE_TOPICS = new Set([
  'ARK', 'TEMPLE', 'TABERNACLE', 'ISRAEL', 'CHURCH',
]);

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
  queryTopic: string,
  narrativeContext?: NarrativeContext,
  seedTopicIdSet?: Set<number>,
  topicSemanticScoreById?: Map<number, number>,
  likeTopicIdSet?: Set<number>,
): Promise<MajorWitness[]> {
  const topicIds = expandedTopics.map((t) => t.id);

  // Build book semantic score lookup from vectorize book results.
  const bookSemanticScoreMap = new Map<number, number>();
  for (const { book_id, score } of semanticBooks) {
    bookSemanticScoreMap.set(book_id, score);
  }

  const candidates = await aggregateWitnesses(topicIds);

  // In narrative mode, run a second aggregation restricted to figure-specific seed topics.
  // This gives per-book verse counts from topics directly about the story figure
  // (e.g. NOAH, FLOOD, ANTEDILUVIANS for "Noah and the ark") without polysemous
  // topics like ARK (which covers the Ark of the Covenant in Exodus/Numbers) or
  // co-occurring expansions. Used to:
  //   (a) determine the primary narrative book (highest seed verse count)
  //   (b) filter out non-primary witnesses that have no direct figure-topic verses
  //   (c) constrain the primary book's anchor chapter range to the seed-dense span
  const isNarrativeModeForSeed = narrativeContext?.narrativeMode === true;
  const seedVerseCountByBook = new Map<number, number>();
  const seedCandidates: WitnessCandidate[] = [];
  if (isNarrativeModeForSeed && seedTopicIdSet && seedTopicIdSet.size > 0) {
    // Filter 1: exclude polysemous topics that span multiple unrelated narratives.
    const afterPolysemousFilter = Array.from(seedTopicIdSet).filter((tid) => {
      const topic = expandedTopics.find((t) => t.id === tid);
      if (!topic) return false;
      return !POLYSEMOUS_NARRATIVE_TOPICS.has(topic.name.toUpperCase());
    });

    // Filter 2: when narrativeFigure is known, further restrict to topics whose
    // names contain the figure name. This prevents broad topics like FLOOD from
    // contributing seed-verse counts for books that use the same topic in a
    // different story context (e.g. Numbers flood/water references). Topics that
    // include the figure name (e.g. NOAH, NOAH'S ARK) will have relevance >= 0.4
    // via naveTopicRelevance; unrelated topics like FLOOD or COVENANT will not.
    // Fall back to afterPolysemousFilter if no topics pass the figure filter.
    const narrativeFigure = narrativeContext?.narrativeFigure;
    let filteredSeedIds = afterPolysemousFilter;
    if (narrativeFigure) {
      const figureFiltered = afterPolysemousFilter.filter((tid) => {
        const topic = expandedTopics.find((t) => t.id === tid);
        if (!topic) return false;
        // Require relevance >= 0.6 so the figure name is a complete word
        // in the topic name (exact word match, starts-with, or exact).
        // This rejects substring-only matches like MANOAH, JANOAH, ZANOAH
        // for a "Noah" figure query — those topic names contain "noah" as
        // a substring but are unrelated persons or places.
        return naveTopicRelevance(topic.name, narrativeFigure) >= 0.6;
      });
      if (figureFiltered.length > 0) {
        filteredSeedIds = figureFiltered;
      }
    }

    const seedIdsToUse = filteredSeedIds.length > 0 ? filteredSeedIds : Array.from(seedTopicIdSet);

    const seedAgg = await aggregateWitnesses(seedIdsToUse);
    seedCandidates.push(...seedAgg);

    // Guard: if seed aggregation returned no candidates in narrative mode, log a
    // production-visible error so the failure is traceable. This can happen when
    // the seed topic IDs don't match any verses in D1 (e.g. a new topic added after
    // ETL) or when all seeds were filtered out as polysemous.
    if (seedCandidates.length === 0) {
      console.error(
        `[topical_search] narrative mode: seedCandidates is empty for query="${queryTopic}", ` +
        `seedIdsToUse=[${seedIdsToUse.join(',')}]`,
      );
    }

    // Determine the primary seed candidate (highest verse count) and its chapter range.
    // Use this to validate non-primary OT books: if a non-primary OT book's seed verses
    // are in a completely different chapter range from the primary narrative span, it's
    // a same-name disambiguation (e.g. Numbers 26–36 for "Noah daughter of Zelophehad"
    // when the query is about Noah the patriarch in Genesis 5–9). NT books are exempt
    // from this check since they legitimately cross-reference OT narratives in their own
    // chapter structure (e.g. Heb 11:7, 1 Pet 3:20 referencing Noah).
    const primarySeedBookId = seedCandidates.length > 0 ? seedCandidates[0].book_id : undefined;
    const primarySeedChapterMin = seedCandidates.length > 0 ? seedCandidates[0].min_chapter : undefined;
    const primarySeedChapterMax = seedCandidates.length > 0 ? seedCandidates[0].max_chapter : undefined;
    // "Narrow" narrative: the primary story spans fewer than 15 chapters.
    // This threshold covers self-contained story units (e.g. Genesis 6-9 for Noah,
    // ~4 chapters) while excluding book-wide narratives (e.g. Moses across Exodus,
    // ~40 chapters). Only narrow narratives trigger the chapter-range disambiguation
    // check for secondary OT books — broad narratives may legitimately recur across
    // many chapters in other books, so the overlap check would reject valid witnesses.
    const primaryNarrativeIsNarrow =
      primarySeedChapterMin !== undefined &&
      primarySeedChapterMax !== undefined &&
      primarySeedChapterMax - primarySeedChapterMin < 15;

    for (const sc of seedCandidates) {
      if (
        primaryNarrativeIsNarrow &&
        sc.book_id !== primarySeedBookId &&
        sc.testament === 'OT'
      ) {
        // Check chapter range overlap with primary narrative span.
        // No overlap means this OT book's seed verses are about a different
        // person or event (disambiguation case).
        const overlaps =
          sc.min_chapter <= primarySeedChapterMax! &&
          sc.max_chapter >= primarySeedChapterMin!;
        if (!overlaps) {
          // Exclude from seed counts — this OT book references the topic in
          // a different context (different story, same topic name).
          continue;
        }
      }
      seedVerseCountByBook.set(sc.book_id, sc.verse_count);
    }
  }

  // Count semantic verse hits per book — direct embedding-based relevance signal.
  const semanticHitsPerBook = new Map<number, number>();
  for (const coord of semanticCoords.values()) {
    semanticHitsPerBook.set(
      coord.book_id,
      (semanticHitsPerBook.get(coord.book_id) ?? 0) + 1,
    );
  }

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
  // In narrative mode, non-primary books must clear higher thresholds to prevent
  // loosely related books (e.g. Exodus, Numbers for a Noah query) from qualifying.
  // The primary narrative book is the top seed-verse-count candidate in narrative mode
  // (seedCandidates[0]) — NOT the all-topic candidates[0], which is inflated by
  // co-occurring topics (TABERNACLE, ISRAEL) that drag Exodus to the top for Noah.
  const isNarrativeModeForFilter = narrativeContext?.narrativeMode === true;
  const narrativePrimaryBookIdForFilter = isNarrativeModeForFilter
    ? (seedCandidates.length > 0 ? seedCandidates[0].book_id : candidates[0]?.book_id)
    : undefined;

  const qualified = candidates.filter((c) => {
    if (
      isNarrativeModeForFilter &&
      c.book_id !== narrativePrimaryBookIdForFilter
    ) {
      // Non-primary book in narrative mode: apply stricter thresholds AND
      // require at least one direct seed-topic verse. This eliminates books
      // like Exodus and Numbers for a Noah query (0 NOAH/FLOOD/ARK verses)
      // while preserving Hebrews (Heb 11:7 in NOAH) and 1 Peter (1 Pet 3:20
      // in FLOOD) which have legitimate direct references.
      const seedHits = seedVerseCountByBook.get(c.book_id) ?? 0;
      return (
        seedHits >= 1 &&
        c.verse_count >= MAJOR_WITNESS_MIN_VERSES_NARRATIVE_NON_PRIMARY &&
        c.chapter_count >= MAJOR_WITNESS_MIN_CHAPTERS_NARRATIVE_NON_PRIMARY
      );
    }
    return (
      c.verse_count >= MAJOR_WITNESS_MIN_VERSES &&
      c.chapter_count >= MAJOR_WITNESS_MIN_CHAPTERS
    );
  });

  // Score candidates using five complementary signals:
  //   1. Salience (pre-computed topical centrality) — dominant when available
  //   2. Chapter coverage ratio (matched chapters / total book chapters) —
  //      rewards books where the topic pervades the whole text
  //   3. log2(verse_count) — breadth tiebreaker
  //   4. Book semantic score — Vectorize book-level embedding match
  //   5. Semantic verse hits — how many of the top verse-level embedding
  //      results land in this book (direct query-relevance signal)
  // Pre-compute max semantic hits for normalization in query-alignment scoring.
  const maxSemanticHits = Math.max(0, ...Array.from(semanticHitsPerBook.values()));

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
    // In narrative mode, reduce chapterCoverage dominance so that broad books
    // (e.g. Genesis with 50 chapters) don't outrank tightly story-relevant
    // secondary witnesses (e.g. Hebrews 11, 1 Peter 3 for Noah). Instead,
    // totalSalience is elevated so topical relevance dominates over breadth.
    const salienceWeight = isNarrativeModeForFilter ? 4.5 : 3.0;
    const chapterCoverageWeight = isNarrativeModeForFilter ? 2.0 : 4.0;
    const witnessScore =
      totalSalience * salienceWeight +
      chapterCoverage * chapterCoverageWeight +
      verseBreadth +
      bookSemanticScore * 3.0 +
      semVerseHits * 1.5;

    // Compute query-alignment score as a tiebreaker (does NOT replace witnessScore).
    // This score reflects how well this book's evidence maps to the user's specific
    // query terms — topic keyword relevance weights + normalized semantic verse density.
    const matchedTopicIdsForAlignment = salienceTopicIds.filter((tid) => {
      const topicName = expandedTopics.find((t) => t.id === tid)?.name;
      if (!topicName) return false;
      return candidate.topic_names
        .split(',')
        .map((n) => n.trim())
        .includes(topicName);
    });
    const queryAlignmentScore = computeQueryAlignmentScore(
      candidate.book_id,
      matchedTopicIdsForAlignment,
      topicRelevanceWeight,
      semanticHitsPerBook,
      maxSemanticHits,
    );

    return { candidate, witnessScore, queryAlignmentScore };
  });

  // Primary sort: witnessScore descending.
  // Tiebreaker: queryAlignmentScore descending — within a witnessScore band,
  // prefer books whose evidence is more tightly coupled to the user's query.
  scored.sort((a, b) => {
    if (b.witnessScore !== a.witnessScore) return b.witnessScore - a.witnessScore;
    return b.queryAlignmentScore - a.queryAlignmentScore;
  });
  // Apply both a hard cap and a relative score cutoff so that:
  // - Queries with a clear top-N (large score gaps) return fewer witnesses
  // - Queries with many similarly-scored books (like "end times prophecy")
  //   return more witnesses to capture all relevant books
  //
  // For narrative-mode queries (specific stories/figures):
  //   - Tighter cap (MAX_MAJOR_WITNESSES_NARRATIVE) focuses on story-specific witnesses
  //   - Stricter cutoff (WITNESS_SCORE_CUTOFF_NARRATIVE) filters weak associations
  //   - The primary narrative book is always included regardless of score
  const isNarrativeMode = narrativeContext?.narrativeMode === true;
  const topScore = scored.length > 0 ? scored[0].witnessScore : 0;
  const activeCutoff = isNarrativeMode ? WITNESS_SCORE_CUTOFF_NARRATIVE : WITNESS_SCORE_CUTOFF;
  const activeMaxWitnesses = isNarrativeMode ? MAX_MAJOR_WITNESSES_NARRATIVE : MAX_MAJOR_WITNESSES;
  const cutoff = topScore * activeCutoff;

  // For narrative queries, identify the primary book using seed-only aggregation.
  // In narrative mode, seedCandidates (sorted by seed verse_count desc) gives the
  // book with the most direct story-topic verses — e.g. Genesis for NOAH/FLOOD/ARK.
  // The all-topic candidates[0] can be wrong because co-occurring topics (TABERNACLE,
  // ISRAEL, FAITH) inflate Exodus/Numbers well above Genesis for a Noah query.
  // Fall back to candidates[0] only if no seed candidates exist.
  if (isNarrativeMode && narrativeContext) {
    if (seedCandidates.length > 0) {
      narrativeContext.primaryBookId = seedCandidates[0].book_id;
    } else if (candidates.length > 0) {
      narrativeContext.primaryBookId = candidates[0].book_id;
    }
  }

  const primaryBookId = isNarrativeMode ? narrativeContext?.primaryBookId : undefined;

  // For the primary narrative book, compute the seed-only chapter range.
  // This is the min/max chapter from the seed-topic-only aggregation (e.g.
  // Genesis 5–9 for Noah from NOAH/FLOOD/ARK topics) rather than the broader
  // all-topic range (Genesis 1–50 with co-occurring topics like COVENANT).
  // Used to constrain anchor passages and representative verse selection.
  let primarySeedMin: number | undefined;
  let primarySeedMax: number | undefined;
  if (isNarrativeMode && primaryBookId !== undefined) {
    const primarySeedCandidate = seedCandidates.find((sc) => sc.book_id === primaryBookId);
    if (primarySeedCandidate) {
      primarySeedMin = primarySeedCandidate.min_chapter;
      primarySeedMax = primarySeedCandidate.max_chapter;
    }
  }

  // Build the candidate list: apply cap + cutoff, but always include the primary book.
  const cappedSlice = scored.slice(0, activeMaxWitnesses);
  const topScoredCandidates = isNarrativeMode
    ? cappedSlice.filter(
        (s) => s.witnessScore >= cutoff || s.candidate.book_id === primaryBookId,
      )
    : cappedSlice.filter((s) => s.witnessScore >= cutoff);

  // Build witnesses concurrently (representative verse may need D1 fallback).
  const witnesses: MajorWitness[] = await Promise.all(
    topScoredCandidates.map(async ({ candidate, witnessScore, queryAlignmentScore }) => {
      const matchedTopics = candidate.topic_names
        .split(',')
        .map((n) => n.trim())
        .filter(Boolean);

      // Prefer short topic names as narrative labels.
      const shortTopics = matchedTopics.filter(
        (t) => t.split(/\s+/).length <= 3,
      );
      const narrative = detectNarrative(candidate, shortTopics);

      // Resolve the topic IDs that caused this book to become a witness.
      // These are the salienceTopicIds whose names appear in the candidate's
      // topic_names list. We use them to prefer topic-grounded verses when
      // selecting the representative verse.
      const matchedTopicIds = expandedTopics
        .filter((t) => matchedTopics.includes(t.name))
        .map((t) => t.id);

      // Pass seed chapter range for the primary narrative book so the
      // representative verse is selected from within the story unit (e.g.
      // Genesis 6-9 for Noah) rather than any topic-matched chapter in the book.
      const seedChapterRangeForVerse =
        isNarrativeMode &&
        candidate.book_id === primaryBookId &&
        primarySeedMin !== undefined &&
        primarySeedMax !== undefined
          ? { minChapter: primarySeedMin, maxChapter: primarySeedMax }
          : undefined;

      const representativeVerse = await fetchRepresentativeVerse(
        candidate,
        matchedTopicIds,
        semanticCoords,
        semanticVerseMap,
        narrativeContext,
        seedChapterRangeForVerse,
      );

      // Build enrichment fields from in-memory data (no additional D1 queries).
      // Compute themesMatched first so it can inform the narrative explanations.
      // Pass seedTopicIdSet so co-occurring topics use the stricter 0.85 threshold.
      // Pass topicSemanticScoreById and likeTopicIdSet so co-occurring topics also
      // require a minimum semantic similarity score to the query — filtering tangential
      // themes like "the church" from suffering queries.
      const themesMatched = buildThemesMatched(
        candidate,
        expandedTopics,
        salienceMap,
        salienceTopicIds,
        seedTopicIdSet,
        topicSemanticScoreById,
        likeTopicIdSet,
      );

      const whyThisBookMatters = buildWhyThisBookMatters(
        candidate,
        salienceMap,
        salienceTopicIds,
        expandedTopics,
        themesMatched,
      );

      const totalBookChapters = BOOK_TOTAL_CHAPTERS[candidate.book_name] ?? candidate.chapter_count;
      const rawAnchorRows = anchorPassageData.get(candidate.book_id) ?? [];
      // In narrative mode, constrain the anchor data for the primary narrative
      // book to only chapters within the story's span. Use the seed-only chapter
      // range (primarySeedMin/Max) when available — this tightens the anchor to
      // e.g. Genesis 6-9 for Noah rather than the all-topic range which could
      // include Genesis 1-50 via co-occurring topics like COVENANT or SACRIFICE.
      // Falls back to the all-topic candidate range if seed range is unavailable.
      const anchorRows =
        isNarrativeMode && candidate.book_id === primaryBookId
          ? rawAnchorRows.filter(
              (r) =>
                r.chapter >= (primarySeedMin ?? candidate.min_chapter) &&
                r.chapter <= (primarySeedMax ?? candidate.max_chapter),
            )
          : rawAnchorRows;
      const candidateGenre = BOOK_GENRE[candidate.book_name];
      const suggestedAnchorPassages = clusterToPassageRanges(
        anchorRows,
        candidate.book_name,
        totalBookChapters,
        candidateGenre,
        queryTopic,
      );

      const narrativeReason = buildNarrativeReason(narrative, candidate, themesMatched);

      // Classify witness strength relative to the top-scoring witness.
      // central    >= 0.85 of top score — reserved for the most relevant books
      // strong     >= 0.65 of top score — clearly relevant secondary witnesses
      // supporting >= 0.45 of top score — genuinely related but less central
      let witness_strength: MajorWitness['witness_strength'];
      if (topScore === 0 || witnessScore / topScore >= 0.85) {
        witness_strength = 'central';
      } else if (witnessScore / topScore >= 0.65) {
        witness_strength = 'strong';
      } else {
        witness_strength = 'supporting';
      }

      // Build a human-readable explanation connecting this book's evidence to
      // the user's specific query. Uses the query-alignment score and per-book
      // semantic hit count already computed during witness scoring.
      const semanticHits = semanticHitsPerBook.get(candidate.book_id) ?? 0;
      const queryAlignmentNote = buildQueryAlignmentNote(
        candidate.book_name,
        queryTopic,
        themesMatched,
        semanticHits,
        queryAlignmentScore,
      );

      const witness: MajorWitness = {
        book: candidate.book_name,
        testament: candidate.testament,
        genre: BOOK_GENRE[candidate.book_name] ?? 'Unknown',
        verse_count: candidate.verse_count,
        chapter_count: candidate.chapter_count,
        matched_topics: matchedTopics.slice(0, 10),
        match_reason: buildWitnessMatchReason(candidate, narrative, themesMatched),
        witness_strength,
        representative_verse: representativeVerse,
        why_this_book_matters: whyThisBookMatters,
        themes_matched: themesMatched,
        suggested_anchor_passages: suggestedAnchorPassages,
        query_alignment_note: queryAlignmentNote,
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

  // Detect whether the query names a specific biblical narrative or figure.
  // This is done after LIKE topics resolve so we can use them as a signal.
  const narrativeContext = detectQueryNarrative(topic, likeTopics);

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

  // Phase 3 (was Phase 2): Build major witnesses using semantic topics + book scores + salience.
  // Build seedTopicIdSet from the pre-expansion seed topic IDs so buildMajorWitnesses
  // can distinguish direct query matches (seed) from co-occurring expansions.
  const seedTopicIdSet = new Set(seedTopicIds);
  const majorWitnesses =
    finalExpandedTopics.length > 0
      ? await buildMajorWitnesses(finalExpandedTopics, salienceTopicIds, topicRelevanceWeight, semanticBooks, semanticCoords, semanticVerseMap, topic, narrativeContext ?? undefined, seedTopicIdSet, semanticScoreById, likeTopicIdSet)
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
  'Major witnesses include witness_strength (central/strong/supporting) and query_alignment_note to explain how the book\'s evidence connects to your query. ' +
  'For story/narrative queries (e.g., "Noah and the ark", "the Exodus", "David and Goliath"): topical_search identifies the primary narrative book and anchor verse range. ' +
  'After getting results, pick a representative verse from the story unit (cross_references takes a single verse, not a range), then use cross_references on that anchor verse to find how other books reference the story, and use semantic_search for verse-level exploration within the story unit. ' +
  'For deeper study: use cross_references to expand a verse found in the results, semantic_search for additional verse-level discovery, find_text for keyword-based follow-up searches, or word_study for original-language analysis of key terms.';

topicalSearch.input = {
  topic: T.string({
    required: true,
    description:
      'The topic to search for. Accepts single topics (e.g., "forgiveness", "prayer") or compound themes (e.g., "God\'s faithfulness during suffering"). ' +
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

// Named exports for unit testing genre-aware explanation templates, theme mapping,
// genre-aware clustering strategies, and query-alignment scoring.
export {
  buildWhyThisBookMatters,
  buildWitnessMatchReason,
  buildNarrativeReason,
  buildThemesMatched,
  toThemeLabel,
  clusterToPassageRanges,
  isConsolationQuery,
  computeQueryAlignmentScore,
  buildQueryAlignmentNote,
};
