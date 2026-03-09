/**
 * Bible MCP Server
 *
 * Built with @mctx-ai/mcp-server. Provides Bible text lookup, semantic search,
 * cross-references, word study, concordance, and topical discovery across 5
 * public domain translations (KJV, WEB, ASV, YLT, Darby).
 */

import { createServer } from '@mctx-ai/mcp-server';

// ─── Lib ──────────────────────────────────────────────────────────────────────
//
// bible-utils exports ensureInitialized(), which each tool and resource handler
// calls at the start of its first request. That call populates the translation
// and book caches from D1 once, then becomes a no-op on subsequent requests.
// The caches are never pre-populated at module load time.

import './lib/bible-utils.js';

// ─── Resources ───────────────────────────────────────────────────────────────

import translationsHandler from './resources/translations.js';
import chapterHandler from './resources/chapter.js';
import verseHandler from './resources/verse.js';

// ─── Tools ───────────────────────────────────────────────────────────────────

import searchBibleHandler from './tools/search-bible.js';
import findTextHandler from './tools/find-text.js';
import compareTranslationsHandler from './tools/compare-translations.js';
import crossReferencesHandler from './tools/cross-references.js';
import wordStudyHandler from './tools/word-study.js';
import concordanceHandler from './tools/concordance.js';
import topicalSearchHandler from './tools/topical-search.js';

// ─── Server ──────────────────────────────────────────────────────────────────

const server = createServer({
  instructions: `Bible MCP Server v1.0.14 — 5 public domain translations: KJV, WEB, ASV, YLT, Darby (case-insensitive). Every result includes a structured citation (book, chapter, verse, translation).
Always cite results as 'Book Chapter:Verse (Translation)'.

TOOL SELECTION GUIDE:
• Look up a known verse → use bible://{translation}/{book}/{chapter}/{verse} resource (returns verse + context)
• Read a full chapter → use bible://{translation}/{book}/{chapter} resource
• Search by MEANING or concept ("hope in suffering", "what does the Bible say about X") → search_bible (semantic/AI)
• Search by TOPIC with curated editorial index → topical_search (Nave's + semantic; theological topics)
• Find an EXACT WORD OR PHRASE in verse text → find_text (FTS keyword search; quick, returns canonical order)
• Survey ALL occurrences of a word, grouped by book → concordance
• Compare how different translations render a passage → compare_translations (all 5 translations side-by-side)
• Find related passages for a verse → cross_references (curated cross-reference database, ordered by confidence)
• Study the Hebrew/Greek word behind an English word in a verse → word_study (Strong's + BDB/Thayer lexicon + morphology)

DISAMBIGUATION:
- search_bible vs find_text: search_bible finds conceptually related verses; find_text requires the exact word/phrase in the text.
- find_text vs concordance: find_text is faster for spot checks; concordance groups all occurrences by book with totals.
- search_bible vs topical_search: topical_search combines Nave's editorial index with semantic search; prefer for classic theological topics. Use search_bible for open-ended queries.
For deep research, chain tools: topical_search → cross_references → word_study.

RESOURCES:
• bible://translations — list all translations with abbreviation, name, year
• bible://{translation}/{book}/{chapter} — full chapter text (sequential reading)
• bible://{translation}/{book}/{chapter}/{verse} — specific verse with surrounding context (targeted lookup)

Book names accept full names, abbreviations, and common aliases (Gen, Matt, 1 Cor, Rev, etc.).`,
});

// ─── Register Resources ───────────────────────────────────────────────────────

server.resource('bible://translations', translationsHandler);
server.resource('bible://{translation}/{book}/{chapter}', chapterHandler);
server.resource('bible://{translation}/{book}/{chapter}/{verse}', verseHandler);

// ─── Register Tools ───────────────────────────────────────────────────────────

server.tool('search_bible', searchBibleHandler);
server.tool('find_text', findTextHandler);
server.tool('compare_translations', compareTranslationsHandler);
server.tool('cross_references', crossReferencesHandler);
server.tool('word_study', wordStudyHandler);
server.tool('concordance', concordanceHandler);
server.tool('topical_search', topicalSearchHandler);

// ─── Export ──────────────────────────────────────────────────────────────────
//
// The fetch handler processes JSON-RPC 2.0 requests over HTTP.
// Compatible with Cloudflare Workers and mctx hosting.

export default { fetch: server.fetch };
