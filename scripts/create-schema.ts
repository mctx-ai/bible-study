#!/usr/bin/env tsx
/**
 * create-schema.ts
 *
 * Creates all D1 database tables, indexes, and constraints for the Bible Study MCP Server.
 *
 * Usage:
 *   npx tsx scripts/create-schema.ts
 *   npm run db:schema
 *
 * Prints all DDL statements to stdout for manual execution via the D1
 * dashboard or wrangler CLI.
 */

import './load-env.js';
import { d1Etl } from '../src/lib/cloudflare-etl.js';

// ---------------------------------------------------------------------------
// DDL statements (order matters — foreign key dependencies)
// ---------------------------------------------------------------------------

const DROP_STATEMENTS: string[] = [
  // Drop in reverse dependency order
  'DROP TABLE IF EXISTS nave_topic_book_salience',
  'DROP TABLE IF EXISTS nave_topic_verses',
  'DROP TABLE IF EXISTS nave_topics',
  'DROP TABLE IF EXISTS morphology',
  'DROP TABLE IF EXISTS lexicon_entries',
  'DROP TABLE IF EXISTS strongs',
  'DROP TABLE IF EXISTS cross_references',
  'DROP TABLE IF EXISTS verses_fts',
  'DROP TABLE IF EXISTS verses',
  'DROP TABLE IF EXISTS book_aliases',
  'DROP TABLE IF EXISTS books',
  'DROP TABLE IF EXISTS translations',
];

const TABLE_STATEMENTS: Record<string, string> = {
  translations: `
    CREATE TABLE IF NOT EXISTS translations (
      id          INTEGER PRIMARY KEY,
      abbreviation TEXT NOT NULL UNIQUE,
      name        TEXT NOT NULL,
      year        INTEGER
    )
  `,

  books: `
    CREATE TABLE IF NOT EXISTS books (
      id              INTEGER PRIMARY KEY,
      abbreviation    TEXT NOT NULL,
      name            TEXT NOT NULL,
      testament       TEXT NOT NULL CHECK(testament IN ('OT', 'NT')),
      canonical_order INTEGER NOT NULL UNIQUE
    )
  `,

  book_aliases: `
    CREATE TABLE IF NOT EXISTS book_aliases (
      alias   TEXT    PRIMARY KEY,
      book_id INTEGER NOT NULL REFERENCES books(id)
    )
  `,

  verses: `
    CREATE TABLE IF NOT EXISTS verses (
      id             INTEGER PRIMARY KEY AUTOINCREMENT,
      book_id        INTEGER NOT NULL,
      chapter        INTEGER NOT NULL,
      verse          INTEGER NOT NULL,
      translation_id INTEGER NOT NULL,
      text           TEXT NOT NULL,
      UNIQUE(book_id, chapter, verse, translation_id)
    )
  `,

  cross_references: `
    CREATE TABLE IF NOT EXISTS cross_references (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      from_book_id  INTEGER NOT NULL,
      from_chapter  INTEGER NOT NULL,
      from_verse    INTEGER NOT NULL,
      to_book_id    INTEGER NOT NULL,
      to_chapter    INTEGER NOT NULL,
      to_verse      INTEGER NOT NULL,
      source        TEXT,
      confidence    REAL
    )
  `,

  strongs: `
    CREATE TABLE IF NOT EXISTS strongs (
      prefixed_number TEXT PRIMARY KEY,
      original_word   TEXT,
      transliteration TEXT,
      definition      TEXT,
      language        TEXT NOT NULL CHECK(language IN ('hebrew', 'greek'))
    )
  `,

  lexicon_entries: `
    CREATE TABLE IF NOT EXISTS lexicon_entries (
      id             INTEGER PRIMARY KEY AUTOINCREMENT,
      strongs_number TEXT NOT NULL,
      language       TEXT NOT NULL,
      short_def      TEXT,
      long_def       TEXT,
      UNIQUE(strongs_number, language, short_def)
    )
  `,

  morphology: `
    CREATE TABLE IF NOT EXISTS morphology (
      id             INTEGER PRIMARY KEY AUTOINCREMENT,
      book_id        INTEGER NOT NULL,
      chapter        INTEGER NOT NULL,
      verse          INTEGER NOT NULL,
      word_position  TEXT NOT NULL,
      strongs_number TEXT,
      raw_strongs    TEXT,
      lemma          TEXT,
      parsing        TEXT,
      translation_id INTEGER NOT NULL,
      UNIQUE(book_id, chapter, verse, word_position, translation_id)
    )
  `,

  nave_topics: `
    CREATE TABLE IF NOT EXISTS nave_topics (
      id               INTEGER PRIMARY KEY AUTOINCREMENT,
      topic_name       TEXT NOT NULL,
      normalized_topic TEXT NOT NULL
    )
  `,

  nave_topic_verses: `
    CREATE TABLE IF NOT EXISTS nave_topic_verses (
      id       INTEGER PRIMARY KEY AUTOINCREMENT,
      topic_id INTEGER NOT NULL,
      book_id  INTEGER NOT NULL,
      chapter  INTEGER NOT NULL,
      verse    INTEGER NOT NULL,
      note     TEXT,
      UNIQUE(topic_id, book_id, chapter, verse)
    )
  `,

  nave_topic_book_salience: `
    CREATE TABLE IF NOT EXISTS nave_topic_book_salience (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      topic_id      INTEGER NOT NULL,
      book_id       INTEGER NOT NULL,
      salience      REAL NOT NULL,
      verse_count   INTEGER NOT NULL,
      chapter_count INTEGER NOT NULL,
      UNIQUE(topic_id, book_id)
    )
  `,
};

const INDEX_STATEMENTS: Record<string, string> = {
  'idx_verses_book_chapter_verse_translation': `
    CREATE INDEX IF NOT EXISTS idx_verses_book_chapter_verse_translation
    ON verses(book_id, chapter, verse, translation_id)
  `,

  'idx_verses_translation_book_chapter_verse': `
    CREATE INDEX IF NOT EXISTS idx_verses_translation_book_chapter_verse
    ON verses(translation_id, book_id, chapter, verse)
  `,

  'idx_cross_references_from': `
    CREATE INDEX IF NOT EXISTS idx_cross_references_from
    ON cross_references(from_book_id, from_chapter, from_verse)
  `,

  'idx_morphology_verse': `
    CREATE INDEX IF NOT EXISTS idx_morphology_verse
    ON morphology(book_id, chapter, verse, translation_id)
  `,

  'idx_morphology_strongs': `
    CREATE INDEX IF NOT EXISTS idx_morphology_strongs
    ON morphology(strongs_number)
  `,

  'idx_nave_topic_verses_topic': `
    CREATE INDEX IF NOT EXISTS idx_nave_topic_verses_topic
    ON nave_topic_verses(topic_id)
  `,

  'idx_nave_topics_normalized': `
    CREATE INDEX IF NOT EXISTS idx_nave_topics_normalized
    ON nave_topics(normalized_topic)
  `,

  'idx_salience_book_topic': `
    CREATE INDEX IF NOT EXISTS idx_salience_book_topic
    ON nave_topic_book_salience(book_id, salience DESC)
  `,

  'idx_salience_topic': `
    CREATE INDEX IF NOT EXISTS idx_salience_topic
    ON nave_topic_book_salience(topic_id, salience DESC)
  `,
};

// FTS5 content virtual table — avoids duplicating verse text storage.
// "content=verses" tells FTS5 to read content from the verses table.
// "content_rowid=id" maps FTS5 rowids to verses.id.
// NOTE: Use lowercase 'fts5' — D1 is case-sensitive for virtual table module names.
const FTS5_STATEMENT = `
  CREATE VIRTUAL TABLE IF NOT EXISTS verses_fts
  USING fts5(text, content=verses, content_rowid=id)
`;

// FTS5 sync triggers — keep the index up-to-date as verses are inserted/updated/deleted.
const FTS5_TRIGGER_STATEMENTS: Record<string, string> = {
  'verses_fts_ai': `
    CREATE TRIGGER IF NOT EXISTS verses_fts_ai
    AFTER INSERT ON verses BEGIN
      INSERT INTO verses_fts(rowid, text) VALUES (new.id, new.text);
    END
  `,

  'verses_fts_ad': `
    CREATE TRIGGER IF NOT EXISTS verses_fts_ad
    AFTER DELETE ON verses BEGIN
      INSERT INTO verses_fts(verses_fts, rowid, text) VALUES ('delete', old.id, old.text);
    END
  `,

  'verses_fts_au': `
    CREATE TRIGGER IF NOT EXISTS verses_fts_au
    AFTER UPDATE ON verses BEGIN
      INSERT INTO verses_fts(verses_fts, rowid, text) VALUES ('delete', old.id, old.text);
      INSERT INTO verses_fts(rowid, text) VALUES (new.id, new.text);
    END
  `,
};

// ---------------------------------------------------------------------------
// Execution helpers
// ---------------------------------------------------------------------------

function normalize(sql: string): string {
  return sql.replace(/\s+/g, ' ').trim();
}

/**
 * Executes SQL statements against the remote D1 database via wrangler.
 * Falls back to printing if D1 credentials are not configured.
 */
async function executeStatements(statements: string[]): Promise<void> {
  const hasCredentials =
    process.env.CLOUDFLARE_API_TOKEN &&
    process.env.CLOUDFLARE_ACCOUNT_ID &&
    process.env.D1_DATABASE_ID;

  if (!hasCredentials) {
    console.log('-- D1 credentials not found. Copy and execute the following SQL via the D1 dashboard or wrangler CLI:\n');
    for (const stmt of statements) {
      console.log(normalize(stmt) + ';\n');
    }
    return;
  }

  const sql = statements.map((s) => normalize(s) + ';').join('\n') + '\n';
  await d1Etl.batchFile(sql);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log('Bible Study MCP Server — D1 Schema Creation');
  console.log('=====================================\n');

  const all: string[] = [];

  // 1. Drop existing tables (for re-runnability)
  console.log('Dropping existing tables...');
  for (const stmt of DROP_STATEMENTS) {
    console.log(`  ${stmt}`);
    all.push(stmt);
  }

  // 2. Create tables
  console.log('\nCreating tables...');
  for (const [name, stmt] of Object.entries(TABLE_STATEMENTS)) {
    console.log(`  CREATE TABLE ${name}`);
    all.push(stmt);
  }

  // 3. Create indexes
  console.log('\nCreating indexes...');
  for (const [name, stmt] of Object.entries(INDEX_STATEMENTS)) {
    console.log(`  CREATE INDEX ${name}`);
    all.push(stmt);
  }

  // 4. Create FTS5 virtual table
  console.log('\nCreating FTS5 virtual table...');
  console.log('  CREATE VIRTUAL TABLE verses_fts USING fts5(...)');
  all.push(FTS5_STATEMENT);

  // 5. Create FTS5 sync triggers
  console.log('\nCreating FTS5 sync triggers...');
  for (const [name, stmt] of Object.entries(FTS5_TRIGGER_STATEMENTS)) {
    console.log(`  CREATE TRIGGER ${name}`);
    all.push(stmt);
  }

  console.log('\nExecuting...\n');
  await executeStatements(all);

  console.log('Done.');
}

main().catch((err) => {
  console.error('Schema creation failed:', err);
  process.exit(1);
});
