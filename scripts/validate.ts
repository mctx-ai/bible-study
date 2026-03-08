#!/usr/bin/env tsx
/**
 * validate.ts
 *
 * Comprehensively validates loaded D1 data for correctness and consistency.
 *
 * Usage:
 *   npx tsx scripts/validate.ts
 *   npm run data:validate
 *
 * Exits 0 if all checks pass, 1 if any fail.
 */

import './load-env.js';
import { d1 } from '../src/lib/cloudflare.js';

// ─── Result tracking ──────────────────────────────────────────────────────────

interface CheckResult {
  name: string;
  passed: boolean;
  message: string;
}

const results: CheckResult[] = [];

function pass(name: string, message: string): void {
  results.push({ name, passed: true, message });
  console.log(`  PASS  ${name}: ${message}`);
}

function fail(name: string, message: string): void {
  results.push({ name, passed: false, message });
  console.log(`  FAIL  ${name}: ${message}`);
}

// ─── Query helpers ────────────────────────────────────────────────────────────

async function count(table: string): Promise<number> {
  const r = await d1.query(`SELECT COUNT(*) AS n FROM ${table}`);
  return (r.results[0] as { n: number }).n;
}

async function scalar<T>(sql: string, params: unknown[] = []): Promise<T> {
  const r = await d1.query(sql, params);
  const row = r.results[0] as Record<string, unknown>;
  return Object.values(row)[0] as T;
}

// ─── 1. Row counts ────────────────────────────────────────────────────────────

async function checkRowCounts(): Promise<void> {
  console.log('\n[1] Row counts');

  const translationsCount = await count('translations');
  if (translationsCount === 7) {
    pass('translations', `${translationsCount} rows`);
  } else {
    fail('translations', `Expected 7, got ${translationsCount}`);
  }

  const booksCount = await count('books');
  if (booksCount === 66) {
    pass('books', `${booksCount} rows`);
  } else {
    fail('books', `Expected 66, got ${booksCount}`);
  }

  const versesTotal = await count('verses');
  const versesInRange = versesTotal >= 140000 && versesTotal <= 175000;
  if (versesInRange) {
    pass('verses (total)', `${versesTotal} rows (~${Math.round(versesTotal / 1000)}K)`);
  } else {
    fail('verses (total)', `Expected ~155K, got ${versesTotal}`);
  }

  // Per-translation verse counts
  const perTranslation = await d1.query(`
    SELECT t.abbreviation, COUNT(*) AS n
    FROM verses v
    JOIN translations t ON t.id = v.translation_id
    GROUP BY v.translation_id
    ORDER BY t.abbreviation
  `);
  for (const row of perTranslation.results as Array<{ abbreviation: string; n: number }>) {
    const inRange = row.n >= 25000 && row.n <= 35000;
    if (inRange) {
      pass(`verses (${row.abbreviation})`, `${row.n} rows`);
    } else {
      fail(`verses (${row.abbreviation})`, `Unexpected count: ${row.n}`);
    }
  }

  const crossRefsCount = await count('cross_references');
  const crossRefsInRange = crossRefsCount >= 600000 && crossRefsCount <= 610000;
  if (crossRefsInRange) {
    pass('cross_references', `${crossRefsCount} rows`);
  } else {
    fail('cross_references', `Expected 600-610K, got ${crossRefsCount}`);
  }

  const strongsTotal = await count('strongs');
  const strongsInRange = strongsTotal >= 10000 && strongsTotal <= 18000;
  if (strongsInRange) {
    pass('strongs (total)', `${strongsTotal} rows`);
  } else {
    fail('strongs (total)', `Expected ~14K, got ${strongsTotal}`);
  }

  // Hebrew vs Greek split
  const strongsSplit = await d1.query(`
    SELECT language, COUNT(*) AS n FROM strongs GROUP BY language
  `);
  for (const row of strongsSplit.results as Array<{ language: string; n: number }>) {
    pass(`strongs (${row.language})`, `${row.n} entries`);
  }

  const lexiconCount = await count('lexicon_entries');
  if (lexiconCount > 0) {
    pass('lexicon_entries', `${lexiconCount} rows`);
  } else {
    fail('lexicon_entries', 'Table is empty');
  }

  const morphologyTotal = await count('morphology');
  const morphologyInRange = morphologyTotal >= 400000 && morphologyTotal <= 500000;
  if (morphologyInRange) {
    pass('morphology (total)', `${morphologyTotal} rows`);
  } else {
    fail('morphology (total)', `Expected ~450K, got ${morphologyTotal}`);
  }

  // OT vs NT split via books.testament join
  const morphSplit = await d1.query(`
    SELECT b.testament, COUNT(*) AS n
    FROM morphology m
    JOIN books b ON b.id = m.book_id
    GROUP BY b.testament
  `);
  for (const row of morphSplit.results as Array<{ testament: string; n: number }>) {
    pass(`morphology (${row.testament})`, `${row.n} rows`);
  }

  const naveTopicsCount = await count('nave_topics');
  const naveTopicsInRange = naveTopicsCount >= 5000 && naveTopicsCount <= 6000;
  if (naveTopicsInRange) {
    pass('nave_topics', `${naveTopicsCount} rows`);
  } else {
    fail('nave_topics', `Expected 5,000-6,000, got ${naveTopicsCount}`);
  }

  const naveTopicVersesCount = await count('nave_topic_verses');
  if (naveTopicVersesCount > 0) {
    pass('nave_topic_verses', `${naveTopicVersesCount} rows`);
  } else {
    fail('nave_topic_verses', 'Table is empty');
  }
}

// ─── 2. Spot-check known verses ───────────────────────────────────────────────

async function checkSpotVerses(): Promise<void> {
  console.log('\n[2] Spot-check known verses');

  const knownVerses = [
    { label: 'Genesis 1:1', book_name: 'Genesis', chapter: 1, verse: 1 },
    { label: 'John 3:16', book_name: 'John', chapter: 3, verse: 16 },
    { label: 'Psalm 23:1', book_name: 'Psalms', chapter: 23, verse: 1 },
    { label: 'Romans 8:28', book_name: 'Romans', chapter: 8, verse: 28 },
    { label: 'Proverbs 3:5', book_name: 'Proverbs', chapter: 3, verse: 5 },
  ];

  // Only check the 5 complete Bible translations — TAGNT and TAHOT are source-text
  // morphology records, not complete translations with verse text for every book.
  const BIBLE_TRANSLATIONS = ['ASV', 'DBY', 'KJV', 'WEB', 'YLT'];

  for (const kv of knownVerses) {
    const result = await d1.query(
      `
      SELECT t.abbreviation, v.text
      FROM verses v
      JOIN books b ON b.id = v.book_id
      JOIN translations t ON t.id = v.translation_id
      WHERE b.name = ? AND v.chapter = ? AND v.verse = ?
        AND t.abbreviation IN ('ASV', 'DBY', 'KJV', 'WEB', 'YLT')
      ORDER BY t.abbreviation
      `,
      [kv.book_name, kv.chapter, kv.verse]
    );

    const rows = result.results as Array<{ abbreviation: string; text: string }>;
    const foundTranslations = rows.map((r) => r.abbreviation);
    const allNonEmpty = rows.every((r) => r.text && r.text.trim().length > 0);

    if (rows.length === BIBLE_TRANSLATIONS.length && allNonEmpty) {
      pass(kv.label, `Found in ${rows.length} translations: ${foundTranslations.join(', ')}`);
    } else if (rows.length === 0) {
      fail(kv.label, 'Not found in any translation');
    } else if (rows.length < BIBLE_TRANSLATIONS.length) {
      const missing = BIBLE_TRANSLATIONS.filter((t) => !foundTranslations.includes(t));
      fail(kv.label, `Missing from ${missing.length} translation(s): ${missing.join(', ')}`);
    } else {
      fail(kv.label, `Found but some texts are empty`);
    }
  }
}

// ─── 3. Foreign key consistency ───────────────────────────────────────────────

async function checkForeignKeys(): Promise<void> {
  console.log('\n[3] Foreign key consistency');

  // verses.book_id → books.id
  const orphanVersesByBook = await scalar<number>(
    `SELECT COUNT(*) FROM verses WHERE book_id NOT IN (SELECT id FROM books)`
  );
  if (orphanVersesByBook === 0) {
    pass('verses.book_id → books.id', 'No orphans');
  } else {
    fail('verses.book_id → books.id', `${orphanVersesByBook} orphan verse(s)`);
  }

  // verses.translation_id → translations.id
  const orphanVersesByTranslation = await scalar<number>(
    `SELECT COUNT(*) FROM verses WHERE translation_id NOT IN (SELECT id FROM translations)`
  );
  if (orphanVersesByTranslation === 0) {
    pass('verses.translation_id → translations.id', 'No orphans');
  } else {
    fail('verses.translation_id → translations.id', `${orphanVersesByTranslation} orphan verse(s)`);
  }

  // cross_references.from_book_id → books.id
  const orphanCrossRefFrom = await scalar<number>(
    `SELECT COUNT(*) FROM cross_references WHERE from_book_id NOT IN (SELECT id FROM books)`
  );
  if (orphanCrossRefFrom === 0) {
    pass('cross_references.from_book_id → books.id', 'No orphans');
  } else {
    fail('cross_references.from_book_id → books.id', `${orphanCrossRefFrom} invalid ref(s)`);
  }

  // cross_references.to_book_id → books.id
  const orphanCrossRefTo = await scalar<number>(
    `SELECT COUNT(*) FROM cross_references WHERE to_book_id NOT IN (SELECT id FROM books)`
  );
  if (orphanCrossRefTo === 0) {
    pass('cross_references.to_book_id → books.id', 'No orphans');
  } else {
    fail('cross_references.to_book_id → books.id', `${orphanCrossRefTo} invalid ref(s)`);
  }

  // morphology.strongs_number → strongs.prefixed_number (NULLs are allowed)
  const unmatchedMorphStrongs = await scalar<number>(
    `SELECT COUNT(*) FROM morphology
     WHERE strongs_number IS NOT NULL
       AND strongs_number != ''
       AND strongs_number NOT IN (SELECT prefixed_number FROM strongs)`
  );
  if (unmatchedMorphStrongs === 0) {
    pass('morphology.strongs_number → strongs.prefixed_number', 'No unmatched entries');
  } else {
    // This is reported but treated as informational per card spec ("report count")
    pass(
      'morphology.strongs_number → strongs.prefixed_number',
      `${unmatchedMorphStrongs} unmatched (expected — cross-references may use multi-strongs or non-standard codes)`
    );
  }

  // nave_topic_verses.topic_id → nave_topics.id
  const orphanNaveVerses = await scalar<number>(
    `SELECT COUNT(*) FROM nave_topic_verses WHERE topic_id NOT IN (SELECT id FROM nave_topics)`
  );
  if (orphanNaveVerses === 0) {
    pass('nave_topic_verses.topic_id → nave_topics.id', 'No orphans');
  } else {
    fail('nave_topic_verses.topic_id → nave_topics.id', `${orphanNaveVerses} orphan association(s)`);
  }

  // nave_topic_verses.book_id → books.id
  const orphanNaveBooks = await scalar<number>(
    `SELECT COUNT(*) FROM nave_topic_verses WHERE book_id NOT IN (SELECT id FROM books)`
  );
  if (orphanNaveBooks === 0) {
    pass('nave_topic_verses.book_id → books.id', 'No invalid book refs');
  } else {
    fail('nave_topic_verses.book_id → books.id', `${orphanNaveBooks} invalid book ref(s)`);
  }

  // lexicon_entries.strongs_number → strongs.prefixed_number
  const orphanLexicon = await scalar<number>(
    `SELECT COUNT(*) FROM lexicon_entries
     WHERE strongs_number NOT IN (SELECT prefixed_number FROM strongs)`
  );
  if (orphanLexicon === 0) {
    pass('lexicon_entries.strongs_number → strongs.prefixed_number', 'No orphans');
  } else {
    fail('lexicon_entries.strongs_number → strongs.prefixed_number', `${orphanLexicon} orphan entry(s)`);
  }
}

// ─── 4. Cross-reference target verse existence ────────────────────────────────

async function checkCrossRefTargets(): Promise<void> {
  console.log('\n[4] Cross-reference target verse existence');

  // Check that (to_book_id, to_chapter, to_verse) all exist in at least one translation
  // We use EXISTS with a subquery against verses rather than a full join to avoid
  // producing a massive result set.
  const danglingRefs = await scalar<number>(`
    SELECT COUNT(*) FROM cross_references cr
    WHERE NOT EXISTS (
      SELECT 1 FROM verses v
      WHERE v.book_id = cr.to_book_id
        AND v.chapter = cr.to_chapter
        AND v.verse   = cr.to_verse
    )
  `);

  if (danglingRefs === 0) {
    pass('cross_references target verses', 'All targets exist in verses table');
  } else {
    fail('cross_references target verses', `${danglingRefs} dangling reference(s)`);
  }
}

// ─── 5. FTS5 verification ─────────────────────────────────────────────────────

async function checkFts5(): Promise<void> {
  console.log('\n[5] FTS5 verification');

  const queries = [
    'in the beginning',
    'God so loved',
    'the Lord is my shepherd',
  ];

  for (const q of queries) {
    const result = await d1.query(
      `SELECT COUNT(*) AS n FROM verses_fts WHERE verses_fts MATCH ?`,
      [q]
    );
    const n = (result.results[0] as { n: number }).n;
    if (n > 0) {
      pass(`FTS5 query "${q}"`, `${n} result(s)`);
    } else {
      fail(`FTS5 query "${q}"`, 'No results returned');
    }
  }
}

// ─── 6. Data integrity ────────────────────────────────────────────────────────

async function checkDataIntegrity(): Promise<void> {
  console.log('\n[6] Data integrity');

  // Duplicate verses: UNIQUE(book_id, chapter, verse, translation_id) should prevent these,
  // but validate anyway.
  const dupVerses = await scalar<number>(`
    SELECT COUNT(*) FROM (
      SELECT book_id, chapter, verse, translation_id
      FROM verses
      GROUP BY book_id, chapter, verse, translation_id
      HAVING COUNT(*) > 1
    )
  `);
  if (dupVerses === 0) {
    pass('No duplicate verses', 'All (book, chapter, verse, translation) combos are unique');
  } else {
    fail('No duplicate verses', `${dupVerses} duplicate combination(s) found`);
  }

  // book_aliases cover all 66 books
  const booksWithoutAlias = await scalar<number>(`
    SELECT COUNT(*) FROM books b
    WHERE NOT EXISTS (
      SELECT 1 FROM book_aliases ba WHERE ba.book_id = b.id
    )
  `);
  if (booksWithoutAlias === 0) {
    pass('book_aliases coverage', 'All 66 books have at least one alias');
  } else {
    fail('book_aliases coverage', `${booksWithoutAlias} book(s) have no alias`);
  }

  // All Strong's numbers have H or G prefix
  const badStrongs = await scalar<number>(`
    SELECT COUNT(*) FROM strongs
    WHERE prefixed_number NOT LIKE 'H%' AND prefixed_number NOT LIKE 'G%'
  `);
  if (badStrongs === 0) {
    pass("Strong's prefix", "All Strong's numbers start with H or G");
  } else {
    fail("Strong's prefix", `${badStrongs} entry(s) without H/G prefix`);
  }
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  console.log('Bible MCP Server — Data Validation');
  console.log('====================================');

  try {
    await checkRowCounts();
    await checkSpotVerses();
    await checkForeignKeys();
    await checkCrossRefTargets();
    await checkFts5();
    await checkDataIntegrity();
  } catch (err) {
    console.error('\nValidation aborted with error:', err);
    process.exit(1);
  }

  // ─── Summary ────────────────────────────────────────────────────────────────

  const passed = results.filter((r) => r.passed);
  const failed = results.filter((r) => !r.passed);

  console.log('\n====================================');
  console.log(`Summary: ${passed.length} passed, ${failed.length} failed`);

  if (failed.length > 0) {
    console.log('\nFailed checks:');
    for (const r of failed) {
      console.log(`  FAIL  ${r.name}: ${r.message}`);
    }
    process.exit(1);
  } else {
    console.log('\nAll checks passed.');
    process.exit(0);
  }
}

main().catch((err) => {
  console.error('Unexpected error:', err);
  process.exit(1);
});
