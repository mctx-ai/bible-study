/**
 * Bible Study App
 *
 * Built with @mctx-ai/app. Provides Bible text lookup, semantic search,
 * cross-references, word study, concordance, and topical discovery across 5
 * public domain translations (KJV, WEB, ASV, YLT, Darby).
 *
 * Version 1.6.4
 */

import { createServer } from '@mctx-ai/app';

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

Bible Study server for Scripture retrieval, topical research, cross-references, translation comparison, concordance, and original-language word study.

Routing framework — choose tools based on what the user is asking for:
- use find_text when the user knows the wording or phrase they want to find
- use word_study when the user wants the Hebrew or Greek behind a specific word in a specific verse
- use compare_translations when the relevant passage is already known and the goal is to compare wording across translations
- use compare_translations or word_study when the user has a specific verse reference and wants interpretation or deeper understanding of a particular word
- use cross_references when you already have an anchor verse and want to trace related passages across Scripture
- use concordance when the user wants counts, distributions, or all occurrences of a word or phrase
- use semantic_search for exploratory discovery of conceptually similar verses, metaphors, images, or ideas when the user is not asking for the Bible's main teaching or major witnesses on a topic
- use topical_search for broad theological questions, especially "What does the Bible say about X?" questions, and whenever the answer should include major witnesses across passages, narratives, books, or genres

Workflow guidance:

For broad topical questions, prefer topical_search over semantic_search, especially when the answer may depend on major books, narratives, or canonical patterns rather than one verse.

If a broad topic was attempted with semantic_search and the results seem narrow, repetitive, verse-fragment oriented, or too dependent on adjacent wording, switch to topical_search rather than retrying semantic search with paraphrased wording.

When building a layered answer: start with topical_search, then optionally expand strong anchor verses with cross_references, then refine with compare_translations or word_study.
1. identify major witnesses with topical_search
2. use direct verse or passage hits from that result
3. optionally expand from strong anchor verses with cross_references
4. refine interpretation with compare_translations or word_study only after the right passage has been identified

Prefer answers that combine direct passages with major biblical witnesses when the user asks about a broad theme.

Biblical meaning may be expressed at multiple levels: word, verse, passage, narrative, book, and canonical theme. Do not assume the best answer is always a list of isolated verses.

Reference content:

RESOURCES:
• bible://translations — list all translations
• bible://{translation}/{book}/{chapter} — full chapter
• bible://{translation}/{book}/{chapter}/{verse} — specific verse with surrounding context

Book names accept full names, abbreviations, and common aliases (Gen, Matt, 1 Cor, Rev).`,
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
