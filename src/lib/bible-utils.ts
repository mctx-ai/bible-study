// Shared types and cached lookups for all Bible MCP tools and resources.
// Caches are populated at module load time via init(); all handlers read from
// in-memory Maps so D1 is never queried per-request for these tables.

import { d1 } from './cloudflare.js';

// ─── Exported types ────────────────────────────────────────────────────────────

export interface Citation {
  book: string; // Full book name (e.g. 'Genesis')
  chapter: number;
  verse: number;
  translation: string; // Abbreviation (e.g. 'KJV')
}

export interface Translation {
  id: number;
  abbreviation: string;
  name: string;
  year: number;
}

export interface Book {
  id: number;
  abbreviation: string;
  name: string;
  testament: string;
  canonical_order: number;
}

// ─── Module-scoped caches ─────────────────────────────────────────────────────

// Keys are uppercase abbreviations.
const translationCache = new Map<string, Translation>();

// Keys are uppercase aliases / book names.
const bookCache = new Map<string, Book>();

// ─── Translations cache ───────────────────────────────────────────────────────

async function loadTranslations(): Promise<void> {
  const result = await d1.query(
    'SELECT id, abbreviation, name, year FROM translations'
  );

  for (const row of result.results) {
    const t: Translation = {
      id: row['id'] as number,
      abbreviation: row['abbreviation'] as string,
      name: row['name'] as string,
      year: row['year'] as number,
    };
    translationCache.set(t.abbreviation.toUpperCase(), t);
  }
}

export function getTranslation(abbrev: string): Translation | undefined {
  return translationCache.get(abbrev.toUpperCase());
}

export function getAllTranslations(): Translation[] {
  return Array.from(translationCache.values());
}

export function isValidTranslation(abbrev: string): boolean {
  return translationCache.has(abbrev.toUpperCase());
}

// ─── Book alias resolver ──────────────────────────────────────────────────────

async function loadBooks(): Promise<void> {
  // Load canonical book names first.
  const booksResult = await d1.query(
    'SELECT id, abbreviation, name, testament, canonical_order FROM books'
  );

  const booksById = new Map<number, Book>();

  for (const row of booksResult.results) {
    const b: Book = {
      id: row['id'] as number,
      abbreviation: row['abbreviation'] as string,
      name: row['name'] as string,
      testament: row['testament'] as string,
      canonical_order: row['canonical_order'] as number,
    };
    booksById.set(b.id, b);
    // Index by canonical name and abbreviation.
    bookCache.set(b.name.toUpperCase(), b);
    bookCache.set(b.abbreviation.toUpperCase(), b);
  }

  // Load aliases and index them.
  const aliasResult = await d1.query(
    'SELECT alias, book_id FROM book_aliases'
  );

  for (const row of aliasResult.results) {
    const bookId = row['book_id'] as number;
    const alias = row['alias'] as string;
    const book = booksById.get(bookId);
    if (book) {
      bookCache.set(alias.toUpperCase(), book);
    }
  }
}

export function resolveBook(nameOrAlias: string): Book | null {
  return bookCache.get(nameOrAlias.toUpperCase()) ?? null;
}

// ─── Helper functions ─────────────────────────────────────────────────────────

export function makeCitation(
  book: Book,
  chapter: number,
  verse: number,
  translationAbbrev: string
): Citation {
  return {
    book: book.name,
    chapter,
    verse,
    translation: translationAbbrev,
  };
}

export function validateVerseRef(
  bookName: string,
  chapter: number,
  verse: number
): { book: Book } | { error: string } {
  const book = resolveBook(bookName);

  if (!book) {
    return { error: `Unknown book: "${bookName}"` };
  }

  if (!Number.isInteger(chapter) || chapter < 1) {
    return { error: `Chapter must be a positive integer; got ${chapter}` };
  }

  if (!Number.isInteger(verse) || verse < 1) {
    return { error: `Verse must be a positive integer; got ${verse}` };
  }

  return { book };
}

// ─── Initialization ───────────────────────────────────────────────────────────

export async function init(): Promise<void> {
  const apiToken =
    process.env.BIBLE_API_TOKEN ?? process.env.CLOUDFLARE_API_TOKEN;
  const accountId =
    process.env.BIBLE_ACCOUNT_ID ?? process.env.CLOUDFLARE_ACCOUNT_ID;
  const databaseId = process.env.D1_DATABASE_ID;

  if (!apiToken || !accountId || !databaseId) {
    console.warn(
      '[bible-utils] D1 env vars not set (BIBLE_API_TOKEN or CLOUDFLARE_API_TOKEN, BIBLE_ACCOUNT_ID or CLOUDFLARE_ACCOUNT_ID, D1_DATABASE_ID). ' +
        'Skipping cache pre-population. Bible lookups will fail at runtime.'
    );
    return;
  }

  await Promise.all([loadTranslations(), loadBooks()]);

  console.log(
    `[bible-utils] Cache ready: ${translationCache.size} translations, ${bookCache.size} book entries`
  );
}

// Pre-populate caches at module load time.
init().catch((err: unknown) => {
  console.error('[bible-utils] Cache initialization failed:', err);
});
