/**
 * Bible Study MCP Server
 *
 * Built with @mctx-ai/mcp-server. Provides Bible text lookup, semantic search,
 * cross-references, word study, concordance, and topical discovery across 5
 * public domain translations (KJV, WEB, ASV, YLT, Darby).
 *
 * Version 1.6.4
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

5 translations: KJV, WEB, ASV, YLT, DBY (case-insensitive). Always cite results as "Book Chapter:Verse (Translation)".

TOOLS:
• topical_search — the best tool for "what does the Bible say about X?" questions. Combines Nave's curated index + semantic search; returns major thematic witnesses (books and narratives) alongside individual verses with match explanations. Works for single topics and compound themes. When results include major_witnesses, present them prominently — they represent the Bible's principal treatments of the topic.
• semantic_search — exploratory vector similarity search when you don't know which passages are relevant. Filters: translation, book, testament.
• find_text — exact keyword or phrase search in verse text
• concordance — count and locate every occurrence of a word or phrase grouped by book with counts
• compare_translations — same passage side-by-side across all translations
• cross_references — related passages for a specific verse (606K references)
• word_study — Hebrew/Greek original word, Strong's number, lexicon definition, and other occurrences

RESOURCES:
• bible://translations — list all translations
• bible://{translation}/{book}/{chapter} — full chapter
• bible://{translation}/{book}/{chapter}/{verse} — specific verse with surrounding context

For questions about what the Bible teaches on a topic (theological themes, compound questions like "God's faithfulness during suffering"), use topical_search. Use semantic_search only for exploratory similarity searches when topical_search is not a good fit. Use find_text for exact wording; use concordance when you need all occurrences with book-level totals. Chain tools for deep research: topical_search → cross_references → word_study. For translation study: semantic_search → compare_translations → word_study. If topical_search returns insufficient results for a query, try semantic_search as a fallback for broader similarity matching. If a tool returns no results, retry semantic_search with a broader query before answering from general knowledge. Book names accept full names, abbreviations, and common aliases (Gen, Matt, 1 Cor, Rev).`,
});

// ─── Register Resources ───────────────────────────────────────────────────────

server.resource('bible://translations', translationsHandler);
server.resource('bible://{translation}/{book}/{chapter}', chapterHandler);
server.resource('bible://{translation}/{book}/{chapter}/{verse}', verseHandler);

// ─── Register Tools ───────────────────────────────────────────────────────────

server.tool('semantic_search', searchBibleHandler);
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
