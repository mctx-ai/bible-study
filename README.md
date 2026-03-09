# Bible MCP Server

A complete scholarly Bible study toolkit — semantic search, original-language word studies, translation comparison, cross-reference traversal, and topical research — available as AI-native tools for the first time. The depth that serious Bible software provides, accessible directly inside your AI assistant through natural conversation.

Connect at [bible.mctx.ai](https://bible.mctx.ai) — works with any MCP-compatible AI client.

---

## What Makes This Different

Scholarly Bible study tools have existed for decades as desktop software and websites. This server makes those capabilities available to AI for the first time via the Model Context Protocol — meaning your AI assistant can chain them together in a single conversation. Search semantically across 155,510 verses, follow a word back to its Hebrew or Greek root, pull the BDB or Thayer lexicon definition, trace 606,140 cross-references, and explore Nave's topical index — each result informing the next query, without leaving the conversation or opening a browser.

The server returns structured data the AI can reason about, not HTML pages a human has to read. That distinction makes multi-step research workflows possible: find every verse about covenant in Genesis, compare how all five translations render the key Hebrew term, then pull cross-references for the most significant occurrences — in one request.

---

## Data at a Glance

| Dataset | Scale |
|---|---|
| English translations | 5 complete (KJV, WEB, ASV, YLT, Darby) — 155,510 verses total |
| Semantic search embeddings | 155,510 vector embeddings |
| Cross-references | 606,140 (OpenBible.info dataset) |
| Strong's concordance entries | 17,543 Hebrew and Greek entries |
| Lexicon definitions | 17,543 entries with short and long definitions |
| Morphology records | 447,734 word-level parsing records (OT Hebrew + NT Greek) |
| Nave's Topical Bible | 5,319 categories, 140,654 verse associations |

---

## Tools

### search_bible
Searches by meaning using semantic similarity. Ask a question or describe a concept in natural language — receive ranked passages that match the intent, not just the keywords. Powered by 155K+ vector embeddings. Filter by translation, book, or testament.

### find_text
Full-text keyword search across all translations or a specific one using FTS5. Results are ordered canonically from Genesis to Revelation.

### compare_translations
Returns any verse or passage in all five translations side by side, making translation choices and textual differences immediately visible.

### cross_references
Finds related passages for a given verse from a dataset of 606,140 cross-references — other parts of scripture that illuminate, echo, or expand on the same idea.

### word_study
Original language analysis for a specific word in a verse. Returns the Hebrew or Greek word, its Strong's number, transliteration, BDB or Thayer lexicon definition, morphological parsing, and every other verse where the same word appears.

### concordance
Finds every verse where a given Hebrew or Greek word (by Strong's number) occurs across the entire Bible.

### topical_search
Discovers verses organized by topic using Nave's Topical Bible — 5,319 curated categories covering biblical subjects, persons, and themes with 140,654 verse associations.

---

## Resources

| URI | Description |
|---|---|
| `bible://translations` | Lists all available translations |
| `bible://{translation}/{book}/{chapter}` | Returns a full chapter |
| `bible://{translation}/{book}/{chapter}/{verse}` | Returns a specific verse with context |

Every verse response includes a structured citation: book, chapter, verse number, and translation.

---

## Example Use Cases

**Word studies in original languages** — Look up "love" in John 3:16, see whether the Greek is *agape* or *phileo*, and trace every verse where that same word appears across the New Testament.

**Comparative translation study** — Show how five translations render a passage and where meaningful differences in word choice appear.

**Topical research** — Find what the Bible says about patience, justice, or covenant using Nave's organized topic index across 5,319 categories.

**Semantic search** — Surface passages that speak to a theme even when the exact word is absent. Search by concept, not keyword.

**Sermon and teaching preparation** — Gather cross-references, compare translations, and study original language nuances for a passage — all in one conversation.

---

## How to Connect

Visit [mctx.ai](https://mctx.ai) to subscribe and get connection instructions for your MCP client.
