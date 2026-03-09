#!/usr/bin/env tsx
/**
 * etl-bible-text.ts
 *
 * Transforms and loads Bible text from 5 public domain translations into D1.
 *
 * Translations loaded: KJV, WEB, ASV, YLT, DBY (Darby)
 * Canonical versification: KJV (decision #6)
 *
 * Usage:
 *   npx tsx scripts/etl-bible-text.ts
 *   npm run etl:bible
 *
 * Required environment variables:
 *   CLOUDFLARE_API_TOKEN
 *   CLOUDFLARE_ACCOUNT_ID
 *   D1_DATABASE_ID
 */

import './load-env.js';
import * as fs from 'fs';
import * as path from 'path';
import { d1Etl, buildMultiRowInserts } from '../src/lib/cloudflare-etl.js';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DATA_DIR = path.resolve(process.cwd(), 'data/scrollmapper');

// ---------------------------------------------------------------------------
// Translation metadata
// ---------------------------------------------------------------------------

interface TranslationMeta {
  id: number;
  abbreviation: string;
  name: string;
  year: number;
  csvFile: string;
}

const TRANSLATIONS: TranslationMeta[] = [
  { id: 1, abbreviation: 'KJV', name: 'King James Version', year: 1769, csvFile: 'KJV.csv' },
  { id: 2, abbreviation: 'WEB', name: 'World English Bible', year: 2000, csvFile: 'WEB.csv' },
  { id: 3, abbreviation: 'ASV', name: 'American Standard Version', year: 1901, csvFile: 'ASV.csv' },
  { id: 4, abbreviation: 'YLT', name: "Young's Literal Translation", year: 1898, csvFile: 'YLT.csv' },
  { id: 5, abbreviation: 'DBY', name: 'Darby Bible', year: 1890, csvFile: 'Darby.csv' },
];

// ---------------------------------------------------------------------------
// Book definitions (66 canonical books, KJV order)
// ---------------------------------------------------------------------------

interface BookDef {
  id: number;
  abbreviation: string;
  name: string;
  testament: 'OT' | 'NT';
  // Alternate names that appear in the scrollmapper CSV files
  csvNames: string[];
  // Aliases to insert into book_aliases (common variations)
  aliases: string[];
}

const BOOKS: BookDef[] = [
  // Old Testament
  {
    id: 1, abbreviation: 'Gen', name: 'Genesis', testament: 'OT',
    csvNames: ['Genesis'],
    aliases: ['Gen', 'gen', 'Ge', 'ge', 'Gn', 'gn'],
  },
  {
    id: 2, abbreviation: 'Exod', name: 'Exodus', testament: 'OT',
    csvNames: ['Exodus'],
    aliases: ['Exod', 'exod', 'Ex', 'ex', 'Exo', 'exo'],
  },
  {
    id: 3, abbreviation: 'Lev', name: 'Leviticus', testament: 'OT',
    csvNames: ['Leviticus'],
    aliases: ['Lev', 'lev', 'Le', 'le', 'Lv', 'lv'],
  },
  {
    id: 4, abbreviation: 'Num', name: 'Numbers', testament: 'OT',
    csvNames: ['Numbers'],
    aliases: ['Num', 'num', 'Nu', 'nu', 'Nm', 'nm'],
  },
  {
    id: 5, abbreviation: 'Deut', name: 'Deuteronomy', testament: 'OT',
    csvNames: ['Deuteronomy'],
    aliases: ['Deut', 'deut', 'Dt', 'dt', 'De', 'de', 'Deu', 'deu'],
  },
  {
    id: 6, abbreviation: 'Josh', name: 'Joshua', testament: 'OT',
    csvNames: ['Joshua'],
    aliases: ['Josh', 'josh', 'Jos', 'jos', 'Jsh', 'jsh'],
  },
  {
    id: 7, abbreviation: 'Judg', name: 'Judges', testament: 'OT',
    csvNames: ['Judges'],
    aliases: ['Judg', 'judg', 'Jdg', 'jdg', 'Jg', 'jg'],
  },
  {
    id: 8, abbreviation: 'Ruth', name: 'Ruth', testament: 'OT',
    csvNames: ['Ruth'],
    aliases: ['ruth', 'Ru', 'ru', 'Rth', 'rth'],
  },
  {
    id: 9, abbreviation: '1Sam', name: '1 Samuel', testament: 'OT',
    csvNames: ['1 Samuel', '1Samuel', 'I Samuel'],
    aliases: ['1Samuel', '1Sam', '1sam', '1Sa', '1sa', '1 Sam', '1s', 'I Samuel'],
  },
  {
    id: 10, abbreviation: '2Sam', name: '2 Samuel', testament: 'OT',
    csvNames: ['2 Samuel', '2Samuel', 'II Samuel'],
    aliases: ['2Samuel', '2Sam', '2sam', '2Sa', '2sa', '2 Sam', 'II Samuel'],
  },
  {
    id: 11, abbreviation: '1Kgs', name: '1 Kings', testament: 'OT',
    csvNames: ['1 Kings', '1Kings', 'I Kings'],
    aliases: ['1Kings', '1Kgs', '1kgs', '1Ki', '1ki', '1Kg', '1 Kgs', 'I Kings'],
  },
  {
    id: 12, abbreviation: '2Kgs', name: '2 Kings', testament: 'OT',
    csvNames: ['2 Kings', '2Kings', 'II Kings'],
    aliases: ['2Kings', '2Kgs', '2kgs', '2Ki', '2ki', '2Kg', '2 Kgs', 'II Kings'],
  },
  {
    id: 13, abbreviation: '1Chr', name: '1 Chronicles', testament: 'OT',
    csvNames: ['1 Chronicles', '1Chronicles', 'I Chronicles'],
    aliases: ['1Chronicles', '1Chr', '1chr', '1Ch', '1ch', '1 Chr', 'I Chronicles'],
  },
  {
    id: 14, abbreviation: '2Chr', name: '2 Chronicles', testament: 'OT',
    csvNames: ['2 Chronicles', '2Chronicles', 'II Chronicles'],
    aliases: ['2Chronicles', '2Chr', '2chr', '2Ch', '2ch', '2 Chr', 'II Chronicles'],
  },
  {
    id: 15, abbreviation: 'Ezra', name: 'Ezra', testament: 'OT',
    csvNames: ['Ezra'],
    aliases: ['ezra', 'Ezr', 'ezr'],
  },
  {
    id: 16, abbreviation: 'Neh', name: 'Nehemiah', testament: 'OT',
    csvNames: ['Nehemiah'],
    aliases: ['Neh', 'neh', 'Ne', 'ne'],
  },
  {
    id: 17, abbreviation: 'Esth', name: 'Esther', testament: 'OT',
    csvNames: ['Esther'],
    aliases: ['Esth', 'esth', 'Es', 'es', 'Est', 'est'],
  },
  {
    id: 18, abbreviation: 'Job', name: 'Job', testament: 'OT',
    csvNames: ['Job'],
    aliases: ['job', 'Jb', 'jb'],
  },
  {
    id: 19, abbreviation: 'Ps', name: 'Psalms', testament: 'OT',
    csvNames: ['Psalms', 'Psalm'],
    aliases: ['Psalm', 'Ps', 'ps', 'Psa', 'psa', 'Pss'],
  },
  {
    id: 20, abbreviation: 'Prov', name: 'Proverbs', testament: 'OT',
    csvNames: ['Proverbs'],
    aliases: ['Prov', 'prov', 'Pr', 'pr', 'Pro', 'pro'],
  },
  {
    id: 21, abbreviation: 'Eccl', name: 'Ecclesiastes', testament: 'OT',
    csvNames: ['Ecclesiastes'],
    aliases: ['Eccl', 'eccl', 'Ec', 'ec', 'Ecc', 'ecc', 'Qoh'],
  },
  {
    id: 22, abbreviation: 'Song', name: 'Song of Solomon', testament: 'OT',
    csvNames: ['Song of Solomon', 'Song of Songs'],
    aliases: ['Song of Songs', 'Song', 'song', 'SoS', 'sos', 'SS', 'Cant'],
  },
  {
    id: 23, abbreviation: 'Isa', name: 'Isaiah', testament: 'OT',
    csvNames: ['Isaiah'],
    aliases: ['Isa', 'isa', 'Is', 'is'],
  },
  {
    id: 24, abbreviation: 'Jer', name: 'Jeremiah', testament: 'OT',
    csvNames: ['Jeremiah'],
    aliases: ['Jer', 'jer', 'Je', 'je', 'Jr', 'jr'],
  },
  {
    id: 25, abbreviation: 'Lam', name: 'Lamentations', testament: 'OT',
    csvNames: ['Lamentations'],
    aliases: ['Lam', 'lam', 'La', 'la'],
  },
  {
    id: 26, abbreviation: 'Ezek', name: 'Ezekiel', testament: 'OT',
    csvNames: ['Ezekiel'],
    aliases: ['Ezek', 'ezek', 'Eze', 'eze', 'Ezk', 'ezk'],
  },
  {
    id: 27, abbreviation: 'Dan', name: 'Daniel', testament: 'OT',
    csvNames: ['Daniel'],
    aliases: ['Dan', 'dan', 'Da', 'da', 'Dn', 'dn'],
  },
  {
    id: 28, abbreviation: 'Hos', name: 'Hosea', testament: 'OT',
    csvNames: ['Hosea'],
    aliases: ['Hos', 'hos', 'Ho', 'ho'],
  },
  {
    id: 29, abbreviation: 'Joel', name: 'Joel', testament: 'OT',
    csvNames: ['Joel'],
    aliases: ['joel', 'Joe', 'joe', 'Jl', 'jl'],
  },
  {
    id: 30, abbreviation: 'Amos', name: 'Amos', testament: 'OT',
    csvNames: ['Amos'],
    aliases: ['amos', 'Am', 'am'],
  },
  {
    id: 31, abbreviation: 'Obad', name: 'Obadiah', testament: 'OT',
    csvNames: ['Obadiah'],
    aliases: ['Obad', 'obad', 'Ob', 'ob', 'Oba', 'oba'],
  },
  {
    id: 32, abbreviation: 'Jonah', name: 'Jonah', testament: 'OT',
    csvNames: ['Jonah'],
    aliases: ['jonah', 'Jon', 'jon', 'Jnh', 'jnh'],
  },
  {
    id: 33, abbreviation: 'Mic', name: 'Micah', testament: 'OT',
    csvNames: ['Micah'],
    aliases: ['Mic', 'mic', 'Mi', 'mi'],
  },
  {
    id: 34, abbreviation: 'Nah', name: 'Nahum', testament: 'OT',
    csvNames: ['Nahum'],
    aliases: ['Nah', 'nah', 'Na', 'na'],
  },
  {
    id: 35, abbreviation: 'Hab', name: 'Habakkuk', testament: 'OT',
    csvNames: ['Habakkuk'],
    aliases: ['Hab', 'hab', 'Hb', 'hb'],
  },
  {
    id: 36, abbreviation: 'Zeph', name: 'Zephaniah', testament: 'OT',
    csvNames: ['Zephaniah'],
    aliases: ['Zeph', 'zeph', 'Zep', 'zep', 'Zp', 'zp'],
  },
  {
    id: 37, abbreviation: 'Hag', name: 'Haggai', testament: 'OT',
    csvNames: ['Haggai'],
    aliases: ['Hag', 'hag', 'Hg', 'hg'],
  },
  {
    id: 38, abbreviation: 'Zech', name: 'Zechariah', testament: 'OT',
    csvNames: ['Zechariah'],
    aliases: ['Zech', 'zech', 'Zec', 'zec', 'Zc', 'zc'],
  },
  {
    id: 39, abbreviation: 'Mal', name: 'Malachi', testament: 'OT',
    csvNames: ['Malachi'],
    aliases: ['Mal', 'mal', 'Ml', 'ml'],
  },
  // New Testament
  {
    id: 40, abbreviation: 'Matt', name: 'Matthew', testament: 'NT',
    csvNames: ['Matthew'],
    aliases: ['Matt', 'matt', 'Mt', 'mt', 'Mat', 'mat'],
  },
  {
    id: 41, abbreviation: 'Mark', name: 'Mark', testament: 'NT',
    csvNames: ['Mark'],
    aliases: ['mark', 'Mk', 'mk', 'Mr', 'mr'],
  },
  {
    id: 42, abbreviation: 'Luke', name: 'Luke', testament: 'NT',
    csvNames: ['Luke'],
    aliases: ['luke', 'Lk', 'lk', 'Lu', 'lu'],
  },
  {
    id: 43, abbreviation: 'John', name: 'John', testament: 'NT',
    csvNames: ['John'],
    aliases: ['john', 'Jn', 'jn', 'Joh', 'joh'],
  },
  {
    id: 44, abbreviation: 'Acts', name: 'Acts', testament: 'NT',
    csvNames: ['Acts'],
    aliases: ['acts', 'Ac', 'ac', 'Act', 'act'],
  },
  {
    id: 45, abbreviation: 'Rom', name: 'Romans', testament: 'NT',
    csvNames: ['Romans'],
    aliases: ['Rom', 'rom', 'Ro', 'ro', 'Rm', 'rm'],
  },
  {
    id: 46, abbreviation: '1Cor', name: '1 Corinthians', testament: 'NT',
    csvNames: ['1 Corinthians', '1Corinthians', 'I Corinthians'],
    aliases: ['1Corinthians', '1Cor', '1cor', '1Co', '1co', '1 Cor', 'I Corinthians'],
  },
  {
    id: 47, abbreviation: '2Cor', name: '2 Corinthians', testament: 'NT',
    csvNames: ['2 Corinthians', '2Corinthians', 'II Corinthians'],
    aliases: ['2Corinthians', '2Cor', '2cor', '2Co', '2co', '2 Cor', 'II Corinthians'],
  },
  {
    id: 48, abbreviation: 'Gal', name: 'Galatians', testament: 'NT',
    csvNames: ['Galatians'],
    aliases: ['Gal', 'gal', 'Ga', 'ga'],
  },
  {
    id: 49, abbreviation: 'Eph', name: 'Ephesians', testament: 'NT',
    csvNames: ['Ephesians'],
    aliases: ['Eph', 'eph', 'Ep', 'ep'],
  },
  {
    id: 50, abbreviation: 'Phil', name: 'Philippians', testament: 'NT',
    csvNames: ['Philippians'],
    aliases: ['Phil', 'phil', 'Php', 'php', 'Pp', 'pp'],
  },
  {
    id: 51, abbreviation: 'Col', name: 'Colossians', testament: 'NT',
    csvNames: ['Colossians'],
    aliases: ['Col', 'col', 'Co', 'co'],
  },
  {
    id: 52, abbreviation: '1Thess', name: '1 Thessalonians', testament: 'NT',
    csvNames: ['1 Thessalonians', '1Thessalonians', 'I Thessalonians'],
    aliases: ['1Thessalonians', '1Thess', '1thess', '1Th', '1th', '1 Thess', 'I Thessalonians'],
  },
  {
    id: 53, abbreviation: '2Thess', name: '2 Thessalonians', testament: 'NT',
    csvNames: ['2 Thessalonians', '2Thessalonians', 'II Thessalonians'],
    aliases: ['2Thessalonians', '2Thess', '2thess', '2Th', '2th', '2 Thess', 'II Thessalonians'],
  },
  {
    id: 54, abbreviation: '1Tim', name: '1 Timothy', testament: 'NT',
    csvNames: ['1 Timothy', '1Timothy', 'I Timothy'],
    aliases: ['1Timothy', '1Tim', '1tim', '1Ti', '1ti', '1 Tim', 'I Timothy'],
  },
  {
    id: 55, abbreviation: '2Tim', name: '2 Timothy', testament: 'NT',
    csvNames: ['2 Timothy', '2Timothy', 'II Timothy'],
    aliases: ['2Timothy', '2Tim', '2tim', '2Ti', '2ti', '2 Tim', 'II Timothy'],
  },
  {
    id: 56, abbreviation: 'Titus', name: 'Titus', testament: 'NT',
    csvNames: ['Titus'],
    aliases: ['titus', 'Tit', 'tit', 'Ti', 'ti'],
  },
  {
    id: 57, abbreviation: 'Phlm', name: 'Philemon', testament: 'NT',
    csvNames: ['Philemon'],
    aliases: ['Phlm', 'phlm', 'Phm', 'phm', 'Pm', 'pm'],
  },
  {
    id: 58, abbreviation: 'Heb', name: 'Hebrews', testament: 'NT',
    csvNames: ['Hebrews'],
    aliases: ['Heb', 'heb', 'He', 'he'],
  },
  {
    id: 59, abbreviation: 'Jas', name: 'James', testament: 'NT',
    csvNames: ['James'],
    aliases: ['Jas', 'jas', 'Ja', 'ja', 'Jms'],
  },
  {
    id: 60, abbreviation: '1Pet', name: '1 Peter', testament: 'NT',
    csvNames: ['1 Peter', '1Peter', 'I Peter'],
    aliases: ['1Peter', '1Pet', '1pet', '1Pe', '1pe', '1 Pet', '1Pt', 'I Peter'],
  },
  {
    id: 61, abbreviation: '2Pet', name: '2 Peter', testament: 'NT',
    csvNames: ['2 Peter', '2Peter', 'II Peter'],
    aliases: ['2Peter', '2Pet', '2pet', '2Pe', '2pe', '2 Pet', '2Pt', 'II Peter'],
  },
  {
    id: 62, abbreviation: '1John', name: '1 John', testament: 'NT',
    csvNames: ['1 John', '1John', 'I John'],
    aliases: ['1John', '1Jn', '1jn', '1Jo', '1jo', 'I John'],
  },
  {
    id: 63, abbreviation: '2John', name: '2 John', testament: 'NT',
    csvNames: ['2 John', '2John', 'II John'],
    aliases: ['2John', '2Jn', '2jn', '2Jo', '2jo', 'II John'],
  },
  {
    id: 64, abbreviation: '3John', name: '3 John', testament: 'NT',
    csvNames: ['3 John', '3John', 'III John'],
    aliases: ['3John', '3Jn', '3jn', '3Jo', '3jo', 'III John'],
  },
  {
    id: 65, abbreviation: 'Jude', name: 'Jude', testament: 'NT',
    csvNames: ['Jude'],
    aliases: ['jude', 'Jud', 'jud'],
  },
  {
    id: 66, abbreviation: 'Rev', name: 'Revelation', testament: 'NT',
    csvNames: ['Revelation', 'Revelation of John'],
    aliases: ['Rev', 'rev', 'Re', 're', 'Rv', 'rv', 'Apoc', 'Revelation of John'],
  },
];

// ---------------------------------------------------------------------------
// Build lookup maps
// ---------------------------------------------------------------------------

/** Map from any CSV book name to its BookDef */
function buildCsvNameToBookMap(): Map<string, BookDef> {
  const map = new Map<string, BookDef>();
  for (const book of BOOKS) {
    for (const csvName of book.csvNames) {
      map.set(csvName, book);
    }
  }
  return map;
}

/** Map from book_id to KJV verse set: "chapter:verse" */
function buildKjvVerseSet(
  parsedKjv: ParsedVerse[]
): Map<number, Set<string>> {
  const map = new Map<number, Set<string>>();
  for (const v of parsedKjv) {
    let set = map.get(v.bookId);
    if (!set) {
      set = new Set();
      map.set(v.bookId, set);
    }
    set.add(`${v.chapter}:${v.verse}`);
  }
  return map;
}

// ---------------------------------------------------------------------------
// CSV parsing
// ---------------------------------------------------------------------------

interface ParsedVerse {
  bookId: number;
  chapter: number;
  verse: number;
  text: string;
}

/**
 * Parse a scrollmapper CSV file.
 * Format: Book,Chapter,Verse,Text
 * Text may be quoted and contain commas.
 */
function parseCsv(csvPath: string, csvNameToBook: Map<string, BookDef>): ParsedVerse[] {
  const content = fs.readFileSync(csvPath, 'utf-8');
  const lines = content.split('\n');
  const verses: ParsedVerse[] = [];

  // Skip header line
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    // Parse CSV: Book,Chapter,Verse,Text
    // Text field may be quoted and contain commas; the first 3 fields never contain commas.
    const firstComma = line.indexOf(',');
    const secondComma = line.indexOf(',', firstComma + 1);
    const thirdComma = line.indexOf(',', secondComma + 1);

    if (firstComma === -1 || secondComma === -1 || thirdComma === -1) {
      log(`WARN  Skipping malformed line ${i + 1}: ${line.slice(0, 80)}`);
      continue;
    }

    const bookName = line.slice(0, firstComma).trim();
    const chapterStr = line.slice(firstComma + 1, secondComma).trim();
    const verseStr = line.slice(secondComma + 1, thirdComma).trim();
    let text = line.slice(thirdComma + 1).trim();

    // Strip surrounding quotes if present
    if (text.startsWith('"') && text.endsWith('"')) {
      text = text.slice(1, -1).replace(/""/g, '"');
    }

    // Apply uniform text cleanup (markup tags, word concatenation, whitespace)
    text = cleanVerseText(text);

    const book = csvNameToBook.get(bookName);
    if (!book) {
      log(`WARN  Unknown book name "${bookName}" at line ${i + 1} — skipping`);
      continue;
    }

    const chapter = parseInt(chapterStr, 10);
    const verse = parseInt(verseStr, 10);

    if (isNaN(chapter) || isNaN(verse)) {
      log(`WARN  Invalid chapter/verse at line ${i + 1}: ${chapterStr}:${verseStr} — skipping`);
      continue;
    }

    verses.push({ bookId: book.id, chapter, verse, text });
  }

  return verses;
}


// ---------------------------------------------------------------------------
// Text cleanup
// ---------------------------------------------------------------------------

/**
 * Clean verse text by removing markup artifacts and normalizing whitespace.
 * Applied uniformly to all translations (no-ops for clean translations).
 *
 * 1. Strip YLT italic markup tags: <FI> and <Fi> (and closing variants)
 * 2. Fix word concatenation before 'God' in Darby/ASV (e.g. 'ForGod' → 'For God')
 * 3. Trim trailing/leading whitespace
 */
function cleanVerseText(text: string): string {
  // 1. Strip YLT italic markup tags (<FI>, </FI>, <Fi>, </Fi>, case-insensitive)
  text = text.replace(/<\/?Fi?>/gi, '');

  // 2. Insert space before 'God' when immediately preceded by a lowercase letter
  //    (fixes 'ForGod', 'AndGod', 'ofGod', 'beginningGod', etc. in Darby/ASV)
  text = text.replace(/([a-z])(God)/g, '$1 $2');

  // 3. Trim whitespace
  return text.trim();
}

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

function log(msg: string): void {
  console.log(`[etl] ${msg}`);
}

// ---------------------------------------------------------------------------
// ETL steps
// ---------------------------------------------------------------------------

async function loadTranslations(): Promise<void> {
  log('Loading translations...');
  const rows = TRANSLATIONS.map((t) => [t.id, t.abbreviation, t.name, t.year]);
  const sql = buildMultiRowInserts(
    'INSERT OR IGNORE INTO translations (id, abbreviation, name, year) VALUES',
    rows
  );
  await d1Etl.batchFile(sql);
  log(`  Inserted ${rows.length} translation records`);
}

async function loadBooks(): Promise<void> {
  log('Loading books...');
  const rows = BOOKS.map((b) => [b.id, b.abbreviation, b.name, b.testament, b.id]);
  const sql = buildMultiRowInserts(
    'INSERT OR IGNORE INTO books (id, abbreviation, name, testament, canonical_order) VALUES',
    rows
  );
  await d1Etl.batchFile(sql);
  log(`  Inserted ${rows.length} book records`);
}

async function loadBookAliases(): Promise<void> {
  log('Loading book aliases...');
  const rows: unknown[][] = [];

  for (const book of BOOKS) {
    // Deduplicate aliases for this book
    const seen = new Set<string>();
    for (const alias of book.aliases) {
      if (seen.has(alias)) continue;
      seen.add(alias);
      rows.push([alias, book.id]);
    }
  }

  const sql = buildMultiRowInserts(
    'INSERT OR IGNORE INTO book_aliases (alias, book_id) VALUES',
    rows
  );
  await d1Etl.batchFile(sql);
  log(`  Inserted ${rows.length} alias records`);
}

async function loadVerses(
  translation: TranslationMeta,
  csvNameToBook: Map<string, BookDef>,
  kjvVerseSet: Map<number, Set<string>> | null
): Promise<number> {
  const csvPath = path.join(DATA_DIR, translation.csvFile);

  if (!fs.existsSync(csvPath)) {
    log(`ERROR  CSV file not found: ${csvPath}`);
    log(`       Run "npm run data:acquire" first.`);
    process.exit(1);
  }

  log(`Parsing ${translation.abbreviation} from ${translation.csvFile}...`);
  const verses = parseCsv(csvPath, csvNameToBook);
  log(`  Parsed ${verses.length} verses`);

  let skipped = 0;
  const rows: unknown[][] = [];

  for (const v of verses) {
    // For non-KJV translations, check that the verse coordinate exists in KJV.
    // Verses outside KJV versification are logged and skipped.
    if (kjvVerseSet !== null) {
      const bookSet = kjvVerseSet.get(v.bookId);
      const key = `${v.chapter}:${v.verse}`;
      if (!bookSet || !bookSet.has(key)) {
        const bookDef = BOOKS.find((b) => b.id === v.bookId);
        log(
          `  VERSIFICATION  ${translation.abbreviation} has ${bookDef?.name ?? v.bookId} ${v.chapter}:${v.verse} — not in KJV, skipping`
        );
        skipped++;
        continue;
      }
    }

    rows.push([v.bookId, v.chapter, v.verse, translation.id, v.text]);
  }

  log(`  Inserting ${rows.length} verses (${skipped} skipped for versification)...`);
  const sql = buildMultiRowInserts(
    'INSERT OR IGNORE INTO verses (book_id, chapter, verse, translation_id, text) VALUES',
    rows
  );
  await d1Etl.batchFile(sql);

  return rows.length;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log('Bible MCP Server — Bible Text ETL');
  console.log('===================================\n');

  // Validate required environment variables
  const required = ['CLOUDFLARE_API_TOKEN', 'CLOUDFLARE_ACCOUNT_ID', 'D1_DATABASE_ID'];
  const missing = required.filter((k) => !process.env[k]);
  if (missing.length > 0) {
    console.error(`ERROR: Missing required environment variables: ${missing.join(', ')}`);
    process.exit(1);
  }

  const csvNameToBook = buildCsvNameToBookMap();

  // Step 1: Load reference metadata
  await loadTranslations();
  await loadBooks();
  await loadBookAliases();

  // Step 2: Load KJV first — it is the canonical versification reference
  const kjvMeta = TRANSLATIONS.find((t) => t.abbreviation === 'KJV')!;
  log(`\nLoading KJV (canonical versification reference)...`);
  const kjvPath = path.join(DATA_DIR, kjvMeta.csvFile);
  if (!fs.existsSync(kjvPath)) {
    log(`ERROR  KJV CSV not found at ${kjvPath}`);
    log(`       Run "npm run data:acquire" first.`);
    process.exit(1);
  }
  const kjvVerses = parseCsv(kjvPath, csvNameToBook);
  const kjvVerseSet = buildKjvVerseSet(kjvVerses);

  const totals: Record<string, number> = {};
  const kjvCount = await loadVerses(kjvMeta, csvNameToBook, null);
  totals[kjvMeta.abbreviation] = kjvCount;

  // Step 3: Load remaining translations, remapping to KJV versification
  for (const translation of TRANSLATIONS) {
    if (translation.abbreviation === 'KJV') continue;
    log(`\nLoading ${translation.abbreviation} — ${translation.name}...`);
    const count = await loadVerses(translation, csvNameToBook, kjvVerseSet);
    totals[translation.abbreviation] = count;
  }

  // Step 4: Summary
  console.log('\n===================================');
  log('ETL complete. Verse totals:');
  for (const [abbr, count] of Object.entries(totals)) {
    log(`  ${abbr.padEnd(6)} ${count.toLocaleString()} verses loaded`);
  }
  log('Done.');
}

main().catch((err) => {
  console.error('[etl] Unexpected error:', err);
  process.exit(1);
});
