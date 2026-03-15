/**
 * Bible Study MCP Server
 *
 * Built with @mctx-ai/mcp-server. Provides Bible text lookup, semantic search,
 * cross-references, word study, concordance, and topical discovery across 5
 * public domain translations (KJV, WEB, ASV, YLT, DBY).
 *
 * Version 1.3.6
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
  instructions: `Always call at least one tool for any question about what the Bible says, teaches, or means. Never answer Bible questions from memory — ground every response in verses retrieved from these tools, then interpret and explain.

6 translations: KJV, WEB, ASV, YLT, DBY, NET (case-insensitive). Always cite results as "Book Chapter:Verse (Translation)".

TOOLS:
• search_bible — semantic/conceptual search ("what does the Bible say about anxiety?")
• topical_search — topic lookup via Nave's curated index + semantic search; returns major thematic witnesses (books and narratives) alongside individual verses with match explanations; best for theological topics (faith, grace, forgiveness). When results include major_witnesses, present them prominently — they represent the Bible's principal treatments of the topic.
• find_text — exact keyword or phrase search in verse text
• concordance — all occurrences of a word or phrase grouped by book with counts
• compare_translations — same passage side-by-side across all 5 translations
• cross_references — related passages for a specific verse (606K references)
• word_study — Hebrew/Greek original word, Strong's number, lexicon definition, and other occurrences

RESOURCES:
• bible://translations — list all translations
• bible://{translation}/{book}/{chapter} — full chapter
• bible://{translation}/{book}/{chapter}/{verse} — specific verse with surrounding context

Prefer topical_search over search_bible for established theological topics. Use find_text for exact wording; use concordance when you need all occurrences with book-level totals. Chain tools for deep research: topical_search → cross_references → word_study. For translation study: search_bible → compare_translations → word_study. If a tool returns no results, retry search_bible with a broader query before answering from general knowledge. Book names accept full names, abbreviations, and common aliases (Gen, Matt, 1 Cor, Rev).`,
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
