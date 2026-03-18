/**
 * Integration tests for the Bible MCP Server.
 *
 * These tests call live Cloudflare APIs (D1, Vectorize, Workers AI) and require
 * CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN environment variables to run.
 * They skip gracefully when credentials are not set.
 */

import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, test, expect } from 'vitest';
import server from './index.js';

// Load .env file for integration test credentials
try {
  const envPath = resolve(process.cwd(), '.env');
  const envFile = readFileSync(envPath, 'utf-8');
  for (const line of envFile.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eqIndex = trimmed.indexOf('=');
    if (eqIndex === -1) continue;
    const key = trimmed.slice(0, eqIndex);
    const value = trimmed.slice(eqIndex + 1);
    if (!process.env[key]) {
      process.env[key] = value;
    }
  }
} catch {
  // .env file not found — integration tests will skip gracefully
}

const hasCredentials = !!(
  process.env.CLOUDFLARE_ACCOUNT_ID &&
  process.env.CLOUDFLARE_API_TOKEN &&
  process.env.D1_DATABASE_ID
);

// Helper to create JSON-RPC 2.0 request
function createRequest(method: string, params: Record<string, unknown> = {}) {
  return new Request('http://localhost', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      jsonrpc: '2.0',
      id: 1,
      method,
      params,
    }),
  });
}

// Helper to parse JSON-RPC response
async function getResponse(response: Response) {
  const data = await response.json();
  return data;
}

// Helper to call a tool and return parsed JSON content
async function callTool(name: string, args: Record<string, unknown>) {
  const req = createRequest('tools/call', { name, arguments: args });
  const res = await server.fetch(req);
  const data = await getResponse(res);
  return data;
}

// Helper to parse the text content from a successful tool response
function parseContent(data: {
  result: { isError?: boolean; content?: { text: string }[] };
}) {
  expect(data.result.isError).toBeFalsy();
  expect(data.result.content).toBeDefined();
  expect(Array.isArray(data.result.content)).toBe(true);
  expect(data.result.content!.length).toBeGreaterThan(0);
  return JSON.parse(data.result.content![0].text);
}

// ─── Integration Tests (require Cloudflare credentials) ─────────────────────

describe.skipIf(!hasCredentials)('Integration: topical_search', () => {
  test('topical_search("suffering") returns results', { timeout: 30_000 }, async () => {
    const data = await callTool('topical_search', { topic: 'suffering' });
    const parsed = parseContent(data);

    expect(Array.isArray(parsed.results)).toBe(true);
    expect(parsed.results.length).toBeGreaterThan(0);
  });

  test('topical_search("love") returns results with major_witnesses', { timeout: 30_000 }, async () => {
    const data = await callTool('topical_search', { topic: 'love' });
    const parsed = parseContent(data);

    expect(Array.isArray(parsed.results)).toBe(true);
    expect(parsed.results.length).toBeGreaterThan(0);
    expect(Array.isArray(parsed.major_witnesses)).toBe(true);
    expect(parsed.major_witnesses.length).toBeGreaterThan(0);
  });

  test('topical_search("faith") returns results', { timeout: 30_000 }, async () => {
    const data = await callTool('topical_search', { topic: 'faith' });
    const parsed = parseContent(data);

    expect(Array.isArray(parsed.results)).toBe(true);
    expect(parsed.results.length).toBeGreaterThan(0);
  });

  test('response contains expected structure: results and major_witnesses arrays', { timeout: 30_000 }, async () => {
    const data = await callTool('topical_search', { topic: 'grace' });
    const parsed = parseContent(data);

    // Verify top-level structure
    expect(Array.isArray(parsed.results)).toBe(true);
    expect(Array.isArray(parsed.major_witnesses)).toBe(true);

    // Verify verse result structure (citation is a nested object)
    if (parsed.results.length > 0) {
      const verse = parsed.results[0];
      expect(verse).toHaveProperty('citation');
      expect(verse.citation).toHaveProperty('book');
      expect(verse.citation).toHaveProperty('chapter');
      expect(verse.citation).toHaveProperty('verse');
    }

    // Verify major witness structure
    if (parsed.major_witnesses.length > 0) {
      const witness = parsed.major_witnesses[0];
      expect(witness).toHaveProperty('book');
      expect(witness).toHaveProperty('match_reason');
    }
  });
});

describe.skipIf(!hasCredentials)('Integration: semantic_search', () => {
  test('semantic_search("hope in suffering") returns results with scores', { timeout: 30_000 }, async () => {
    const data = await callTool('semantic_search', {
      query: 'hope in suffering',
    });
    const parsed = parseContent(data);

    expect(Array.isArray(parsed.results)).toBe(true);
    expect(parsed.results.length).toBeGreaterThan(0);

    // Each result should have a score from vector search
    for (const result of parsed.results) {
      expect(typeof result.score).toBe('number');
    }
  });

  test('semantic_search("love your neighbor") returns results', { timeout: 30_000 }, async () => {
    const data = await callTool('semantic_search', {
      query: 'love your neighbor',
    });
    const parsed = parseContent(data);

    expect(Array.isArray(parsed.results)).toBe(true);
    expect(parsed.results.length).toBeGreaterThan(0);
  });

  test('results contain citation objects with book, chapter, verse', { timeout: 30_000 }, async () => {
    const data = await callTool('semantic_search', {
      query: 'the Lord is my shepherd',
    });
    const parsed = parseContent(data);

    expect(parsed.results.length).toBeGreaterThan(0);
    for (const result of parsed.results) {
      // Grouped results have citation as a nested object
      expect(result.citation).toBeDefined();
      expect(typeof result.citation.book).toBe('string');
      expect(typeof result.citation.chapter).toBe('number');
      expect(typeof result.citation.verse).toBe('number');
    }
  });

  test('results contain translations array', { timeout: 30_000 }, async () => {
    const data = await callTool('semantic_search', { query: 'forgiveness' });
    const parsed = parseContent(data);

    expect(parsed.results.length).toBeGreaterThan(0);
    for (const result of parsed.results) {
      expect(Array.isArray(result.translations)).toBe(true);
      expect(result.translations.length).toBeGreaterThan(0);
    }
  });
});

describe.skipIf(!hasCredentials)('Integration: find_text', () => {
  test('find_text("faith hope") returns results (multi-word AND query)', { timeout: 30_000 }, async () => {
    const data = await callTool('find_text', { query: 'faith hope' });
    const parsed = parseContent(data);

    expect(Array.isArray(parsed.verses)).toBe(true);
    expect(parsed.verses.length).toBeGreaterThan(0);
    expect(parsed.count).toBeGreaterThan(0);
  });

  test('find_text("suffer", translation="WEB") returns results', { timeout: 30_000 }, async () => {
    const data = await callTool('find_text', {
      query: 'suffer',
      translation: 'WEB',
    });
    const parsed = parseContent(data);

    expect(Array.isArray(parsed.verses)).toBe(true);
    expect(parsed.verses.length).toBeGreaterThan(0);
    expect(parsed.count).toBeGreaterThan(0);
  });

  test('find_text("God so loved") returns results (exact phrase preserved)', { timeout: 30_000 }, async () => {
    const data = await callTool('find_text', {
      query: '"God so loved"',
    });
    const parsed = parseContent(data);

    expect(Array.isArray(parsed.verses)).toBe(true);
    expect(parsed.verses.length).toBeGreaterThan(0);
    expect(parsed.count).toBeGreaterThan(0);
  });

  test('find_text("the") returns empty result with message (stop word stripped)', { timeout: 30_000 }, async () => {
    const data = await callTool('find_text', { query: 'the' });
    const parsed = parseContent(data);

    // "the" is a stop word — sanitizeFts5 strips it, leaving no searchable terms
    expect(parsed.count).toBe(0);
    expect(Array.isArray(parsed.verses)).toBe(true);
    expect(parsed.verses.length).toBe(0);
    expect(parsed.message).toBe('No searchable terms found — try more specific keywords.');
  });
});

describe.skipIf(!hasCredentials)('Integration: compare_translations', () => {
  test('compare_translations(Psalms 34:18) returns all 5 translations', { timeout: 30_000 }, async () => {
    const data = await callTool('compare_translations', {
      book: 'Psalms',
      chapter: 34,
      verse_start: 18,
    });
    const parsed = parseContent(data);

    // Response shape: { book, chapter, verse_start, verse_end, verses: [{ verse, translations: [{ text, citation }] }] }
    expect(parsed.book).toBe('Psalms');
    expect(parsed.chapter).toBe(34);
    expect(parsed.verse_start).toBe(18);
    expect(Array.isArray(parsed.verses)).toBe(true);
    expect(parsed.verses.length).toBeGreaterThan(0);

    // Each verse entry should have translations from all 5 translations
    const firstVerse = parsed.verses[0];
    expect(firstVerse.verse).toBe(18);
    expect(Array.isArray(firstVerse.translations)).toBe(true);

    // Collect all translation abbreviations from the first verse's translations
    const translations = new Set<string>();
    for (const entry of firstVerse.translations) {
      expect(typeof entry.text).toBe('string');
      expect(entry.citation).toBeDefined();
      translations.add(entry.citation.translation);
    }

    // Should have all 5 translations
    const expected = ['KJV', 'WEB', 'ASV', 'YLT', 'DBY'];
    for (const t of expected) {
      expect(translations.has(t)).toBe(true);
    }
  });
});
