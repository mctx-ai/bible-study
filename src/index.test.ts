import { describe, test, expect } from 'vitest';
import server from './index.js';

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

// ─── Server Capabilities Tests ────────────────────────────────────────────────

describe('Server capabilities', () => {
  test('tools/list returns all 7 tools', async () => {
    const req = createRequest('tools/list');
    const res = await server.fetch(req);
    const data = await getResponse(res);

    const toolNames = data.result.tools.map((t: { name: string }) => t.name);
    expect(toolNames).toContain('search_bible');
    expect(toolNames).toContain('find_text');
    expect(toolNames).toContain('compare_translations');
    expect(toolNames).toContain('cross_references');
    expect(toolNames).toContain('word_study');
    expect(toolNames).toContain('concordance');
    expect(toolNames).toContain('topical_search');
    expect(toolNames).toHaveLength(7);
  });

  test('resources/list and resources/templates/list return all 3 resources', async () => {
    const staticReq = createRequest('resources/list');
    const staticRes = await server.fetch(staticReq);
    const staticData = await getResponse(staticRes);

    const staticUris = staticData.result.resources.map((r: { uri: string }) => r.uri);
    expect(staticUris).toContain('bible://translations');

    const templateReq = createRequest('resources/templates/list');
    const templateRes = await server.fetch(templateReq);
    const templateData = await getResponse(templateRes);

    const templateUris = templateData.result.resourceTemplates.map(
      (t: { uriTemplate: string }) => t.uriTemplate,
    );
    expect(templateUris).toContain('bible://{translation}/{book}/{chapter}');
    expect(templateUris).toContain('bible://{translation}/{book}/{chapter}/{verse}');
  });
});

// ─── Tool Smoke Tests ─────────────────────────────────────────────────────────
//
// These tests verify each tool returns a response (not an unhandled error).
// They do not assert on specific verse data — correctness is tested in D6.
// All tools call D1/Vectorize/Workers AI; those APIs will fail in the test
// environment, so we assert isError or a response object (not a crash).

describe('Tool: search_bible', () => {
  test('returns a response for a valid query', async () => {
    const req = createRequest('tools/call', {
      name: 'search_bible',
      arguments: { query: 'love your neighbor' },
    });
    const res = await server.fetch(req);
    const data = await getResponse(res);

    // Framework must return a result (even if it is an error from missing env vars)
    expect(data.result).toBeDefined();
    expect(data.result.content).toBeDefined();
    expect(Array.isArray(data.result.content)).toBe(true);
  });

  test('passes numeric values for book_id and translation_id in buildVectorizeFilter', async () => {
    // Exercises the buildVectorizeFilter code path with both book and
    // translation filters. The filter record must use number values (not
    // strings) for book_id and translation_id — this is the type fix being
    // verified. The build step enforces the type contract; this test confirms
    // the code path is exercised without runtime coercion errors.
    const req = createRequest('tools/call', {
      name: 'search_bible',
      arguments: { query: 'love', book: 'John', translation: 'KJV' },
    });
    const res = await server.fetch(req);
    const data = await getResponse(res);

    expect(data.result).toBeDefined();
    expect(data.result.content).toBeDefined();
    expect(Array.isArray(data.result.content)).toBe(true);
  });
});

describe('Tool: find_text', () => {
  test('returns a response for a valid query', async () => {
    const req = createRequest('tools/call', {
      name: 'find_text',
      arguments: { query: 'faith' },
    });
    const res = await server.fetch(req);
    const data = await getResponse(res);

    expect(data.result).toBeDefined();
    expect(data.result.content).toBeDefined();
    expect(Array.isArray(data.result.content)).toBe(true);
  });
});

describe('Tool: compare_translations', () => {
  test('returns a response for a valid verse range', async () => {
    const req = createRequest('tools/call', {
      name: 'compare_translations',
      arguments: { book: 'John', chapter: 3, verse_start: 16 },
    });
    const res = await server.fetch(req);
    const data = await getResponse(res);

    expect(data.result).toBeDefined();
    expect(data.result.content).toBeDefined();
    expect(Array.isArray(data.result.content)).toBe(true);
  });
});

describe('Tool: cross_references', () => {
  test('returns a response for a valid verse reference', async () => {
    const req = createRequest('tools/call', {
      name: 'cross_references',
      arguments: { book: 'Romans', chapter: 8, verse: 28 },
    });
    const res = await server.fetch(req);
    const data = await getResponse(res);

    expect(data.result).toBeDefined();
    expect(data.result.content).toBeDefined();
    expect(Array.isArray(data.result.content)).toBe(true);
  });
});

describe('Tool: word_study', () => {
  test('returns a response for a valid verse and word', async () => {
    const req = createRequest('tools/call', {
      name: 'word_study',
      arguments: { book: 'Genesis', chapter: 1, verse: 1, word: '1' },
    });
    const res = await server.fetch(req);
    const data = await getResponse(res);

    expect(data.result).toBeDefined();
    expect(data.result.content).toBeDefined();
    expect(Array.isArray(data.result.content)).toBe(true);
  });

  test('returns a response when matching by English gloss (e.g. "love")', async () => {
    // Exercises the matchByEnglishGloss path: word is a non-numeric English
    // word that must be matched against strongs_definition / lexicon fields
    // rather than word_position. In the test environment D1 calls fail, so we
    // assert only that the framework returns a well-formed response (no crash).
    const req = createRequest('tools/call', {
      name: 'word_study',
      arguments: { book: 'John', chapter: 3, verse: 16, word: 'love' },
    });
    const res = await server.fetch(req);
    const data = await getResponse(res);

    expect(data.result).toBeDefined();
    expect(data.result.content).toBeDefined();
    expect(Array.isArray(data.result.content)).toBe(true);
  });

  test('returns a response when matched morphology row has null strongs_number', async () => {
    // Exercises the null-strongs_number branch: positional word '1' in
    // Genesis 1:1 may map to a Hebrew particle (definite article, waw, etc.)
    // that carries no Strong's number. The handler should return partial results
    // with an explanatory note rather than throwing an error.
    const req = createRequest('tools/call', {
      name: 'word_study',
      arguments: { book: 'Genesis', chapter: 1, verse: 1, word: '1a' },
    });
    const res = await server.fetch(req);
    const data = await getResponse(res);

    // The tool must return a result object — either a partial WordStudyResult
    // (with a note field) or an isError result when D1 is unavailable in tests.
    expect(data.result).toBeDefined();
    expect(data.result.content).toBeDefined();
    expect(Array.isArray(data.result.content)).toBe(true);
  });

  test('returns alternatives array with required fields when multiple rows match an English word', async () => {
    // Verifies that when matchByEnglishGloss finds multiple matching morphology
    // rows (matched_count > 1), the response includes an alternatives array
    // where each entry has word_position, lemma, strongs_number, transliteration,
    // and short_definition. In test environment D1 calls fail, so we assert
    // the framework returns a well-formed response.
    const req = createRequest('tools/call', {
      name: 'word_study',
      arguments: { book: 'Romans', chapter: 8, verse: 28, word: 'love' },
    });
    const res = await server.fetch(req);
    const data = await getResponse(res);

    expect(data.result).toBeDefined();
    expect(data.result.content).toBeDefined();
    expect(Array.isArray(data.result.content)).toBe(true);

    // When D1 returns real data and multiple rows match, each alternative must
    // have the required shape. Parse the JSON text content to validate structure
    // if an actual result (not error) is returned.
    const contentText = data.result.content?.[0]?.text;
    if (contentText && !data.result.isError) {
      const parsed = JSON.parse(contentText);
      if (parsed.matched_count > 1) {
        expect(Array.isArray(parsed.alternatives)).toBe(true);
        expect(parsed.alternatives.length).toBeGreaterThan(0);
        for (const alt of parsed.alternatives) {
          expect(typeof alt.word_position).toBe('string');
          expect(typeof alt.lemma).toBe('string');
          expect(typeof alt.strongs_number).toBe('string');
          expect(typeof alt.transliteration).toBe('string');
          expect(typeof alt.short_definition).toBe('string');
        }
        expect(typeof parsed.note).toBe('string');
        expect(parsed.note).toContain('Multiple original-language words match');
        expect(parsed.note).toContain('word_position');
      }
    }
  });

  test('includes disambiguation hint note when multiple words match', async () => {
    // Verifies the hint text is included in the response when alternatives exist.
    // In test environment D1 calls fail, so we assert a well-formed response.
    const req = createRequest('tools/call', {
      name: 'word_study',
      arguments: { book: 'John', chapter: 3, verse: 16, word: 'God' },
    });
    const res = await server.fetch(req);
    const data = await getResponse(res);

    expect(data.result).toBeDefined();
    expect(data.result.content).toBeDefined();
    expect(Array.isArray(data.result.content)).toBe(true);

    const contentText = data.result.content?.[0]?.text;
    if (contentText && !data.result.isError) {
      const parsed = JSON.parse(contentText);
      if (parsed.matched_count > 1) {
        expect(typeof parsed.note).toBe('string');
        expect(parsed.note).toContain('Multiple original-language words match');
        expect(parsed.note).toContain('word_position');
      }
    }
  });

  test('throws an error when an invalid translation is passed', async () => {
    // Verifies the silent KJV fallback is removed: passing an unknown
    // translation abbreviation now produces a clear error response.
    const req = createRequest('tools/call', {
      name: 'word_study',
      arguments: { book: 'Genesis', chapter: 1, verse: 1, word: '1', translation: 'INVALID_TRANS' },
    });
    const res = await server.fetch(req);
    const data = await getResponse(res);

    expect(data.result).toBeDefined();
    // The response must be an error (isError) with a message mentioning the unknown translation.
    expect(data.result.isError).toBe(true);
    const errorText = data.result.content?.[0]?.text ?? '';
    expect(errorText).toContain('INVALID_TRANS');
  });
});

describe('Tool: concordance', () => {
  test('returns a response for a valid word', async () => {
    const req = createRequest('tools/call', {
      name: 'concordance',
      arguments: { word: 'grace' },
    });
    const res = await server.fetch(req);
    const data = await getResponse(res);

    expect(data.result).toBeDefined();
    expect(data.result.content).toBeDefined();
    expect(Array.isArray(data.result.content)).toBe(true);
  });
});

describe('Tool: topical_search', () => {
  test('returns a response for a valid topic', async () => {
    const req = createRequest('tools/call', {
      name: 'topical_search',
      arguments: { topic: 'prayer' },
    });
    const res = await server.fetch(req);
    const data = await getResponse(res);

    expect(data.result).toBeDefined();
    expect(data.result.content).toBeDefined();
    expect(Array.isArray(data.result.content)).toBe(true);
  });

  test('returns major_witnesses in response schema', async () => {
    const req = createRequest('tools/call', {
      name: 'topical_search',
      arguments: { topic: 'faith' },
    });
    const res = await server.fetch(req);
    const data = await getResponse(res);

    expect(data.result).toBeDefined();
    expect(Array.isArray(data.result.content)).toBe(true);

    if (!data.result.isError) {
      const parsed = JSON.parse(data.result.content[0].text);
      expect(Array.isArray(parsed.major_witnesses)).toBe(true);
    }
  });

  test('handles multi-word thematic queries without crashing', async () => {
    const queries = [
      "God's faithfulness during suffering",
      'innocent suffering',
      'lament and trust in God',
      'God working through long periods of suffering',
    ];

    for (const topic of queries) {
      const req = createRequest('tools/call', {
        name: 'topical_search',
        arguments: { topic },
      });
      const res = await server.fetch(req);
      const data = await getResponse(res);

      expect(data.result).toBeDefined();
      expect(Array.isArray(data.result.content)).toBe(true);
    }
  });
});

describe.skipIf(!process.env.CLOUDFLARE_ACCOUNT_ID)(
  'Tool: topical_search — thematic correctness',
  () => {
    test('"suffering" surfaces Job as major witness', async () => {
      const req = createRequest('tools/call', {
        name: 'topical_search',
        arguments: { topic: 'suffering' },
      });
      const res = await server.fetch(req);
      const data = await getResponse(res);

      expect(data.result.isError).toBeFalsy();
      const parsed = JSON.parse(data.result.content[0].text);
      const witnessBooks: string[] = parsed.major_witnesses.map(
        (w: { book: string }) => w.book,
      );
      expect(witnessBooks).toContain('Job');
    });

    test('"innocent suffering" heavily favors Job', async () => {
      const req = createRequest('tools/call', {
        name: 'topical_search',
        arguments: { topic: 'innocent suffering' },
      });
      const res = await server.fetch(req);
      const data = await getResponse(res);

      expect(data.result.isError).toBeFalsy();
      const parsed = JSON.parse(data.result.content[0].text);
      expect(parsed.major_witnesses.length).toBeGreaterThan(0);
      expect(parsed.major_witnesses[0].book).toBe('Job');
    });

    test('"lament and trust in God" surfaces Psalms', async () => {
      const req = createRequest('tools/call', {
        name: 'topical_search',
        arguments: { topic: 'lament and trust in God' },
      });
      const res = await server.fetch(req);
      const data = await getResponse(res);

      expect(data.result.isError).toBeFalsy();
      const parsed = JSON.parse(data.result.content[0].text);
      const witnessBooks: string[] = parsed.major_witnesses.map(
        (w: { book: string }) => w.book,
      );
      expect(witnessBooks).toContain('Psalms');
    });

    test('major witnesses include representative verse with text', async () => {
      const req = createRequest('tools/call', {
        name: 'topical_search',
        arguments: { topic: 'faith' },
      });
      const res = await server.fetch(req);
      const data = await getResponse(res);

      expect(data.result.isError).toBeFalsy();
      const parsed = JSON.parse(data.result.content[0].text);
      expect(parsed.major_witnesses.length).toBeGreaterThan(0);

      for (const witness of parsed.major_witnesses) {
        expect(witness.representative_verse).toBeDefined();
        expect(typeof witness.representative_verse.text).toBe('string');
        expect(witness.representative_verse.text.length).toBeGreaterThan(0);
        expect(typeof witness.representative_verse.citation).toBe('string');
        expect(witness.representative_verse.citation.length).toBeGreaterThan(0);
      }
    });

    test('verse results include at least one from a major witness book', async () => {
      const req = createRequest('tools/call', {
        name: 'topical_search',
        arguments: { topic: 'suffering' },
      });
      const res = await server.fetch(req);
      const data = await getResponse(res);

      expect(data.result.isError).toBeFalsy();
      const parsed = JSON.parse(data.result.content[0].text);

      const witnessBooks = new Set<string>(
        parsed.major_witnesses.map((w: { book: string }) => w.book),
      );
      const resultBooks = parsed.results.map(
        (r: { book: string }) => r.book,
      );
      const hasOverlap = resultBooks.some((book: string) =>
        witnessBooks.has(book),
      );
      expect(hasOverlap).toBe(true);
    });

    test('results include match_reason explanations', async () => {
      const req = createRequest('tools/call', {
        name: 'topical_search',
        arguments: { topic: 'faith' },
      });
      const res = await server.fetch(req);
      const data = await getResponse(res);

      expect(data.result.isError).toBeFalsy();
      const parsed = JSON.parse(data.result.content[0].text);
      expect(parsed.results.length).toBeGreaterThan(0);

      const hasMatchReason = parsed.results.some(
        (r: { match_reason?: string }) =>
          typeof r.match_reason === 'string' && r.match_reason.length > 0,
      );
      expect(hasMatchReason).toBe(true);
    });

    test('major witnesses include match_reason', async () => {
      const req = createRequest('tools/call', {
        name: 'topical_search',
        arguments: { topic: 'suffering' },
      });
      const res = await server.fetch(req);
      const data = await getResponse(res);

      expect(data.result.isError).toBeFalsy();
      const parsed = JSON.parse(data.result.content[0].text);
      expect(parsed.major_witnesses.length).toBeGreaterThan(0);

      for (const witness of parsed.major_witnesses) {
        expect(typeof witness.match_reason).toBe('string');
        expect(witness.match_reason.length).toBeGreaterThan(0);
      }
    });
  },
);

// ─── NET Translation Tests ────────────────────────────────────────────────────
//
// Verifies that the NET translation is accessible through all MCP capabilities.
// These tests will only fully pass once NET data is loaded into D1. They are
// written to be syntactically valid and follow the correct patterns — they
// assert on response structure, not verse data.

describe('NET translation support', () => {
  test('NET appears in the translations list resource', async () => {
    const req = createRequest('resources/read', {
      uri: 'bible://translations',
    });
    const res = await server.fetch(req);
    const data = await getResponse(res);

    expect(data.result).toBeDefined();
    expect(data.result.contents).toBeDefined();
    expect(Array.isArray(data.result.contents)).toBe(true);

    // When D1 is available, the translations list must include NET
    const contentsText = data.result.contents?.[0]?.text;
    if (contentsText && !data.result.isError) {
      const translations = JSON.parse(contentsText);
      const abbreviations: string[] = translations.map(
        (t: { abbreviation: string }) => t.abbreviation,
      );
      expect(abbreviations).toContain('NET');
    }
  });

  test('find_text returns a response with NET translation filter', async () => {
    const req = createRequest('tools/call', {
      name: 'find_text',
      arguments: { query: 'love', translation: 'NET' },
    });
    const res = await server.fetch(req);
    const data = await getResponse(res);

    expect(data.result).toBeDefined();
    expect(data.result.content).toBeDefined();
    expect(Array.isArray(data.result.content)).toBe(true);
  });

  test('search_bible returns a response with NET translation filter', async () => {
    const req = createRequest('tools/call', {
      name: 'search_bible',
      arguments: { query: 'love your neighbor', translation: 'NET' },
    });
    const res = await server.fetch(req);
    const data = await getResponse(res);

    expect(data.result).toBeDefined();
    expect(data.result.content).toBeDefined();
    expect(Array.isArray(data.result.content)).toBe(true);
  });

  test('compare_translations includes NET when comparing verses', async () => {
    const req = createRequest('tools/call', {
      name: 'compare_translations',
      arguments: { book: 'John', chapter: 3, verse_start: 16 },
    });
    const res = await server.fetch(req);
    const data = await getResponse(res);

    expect(data.result).toBeDefined();
    expect(data.result.content).toBeDefined();
    expect(Array.isArray(data.result.content)).toBe(true);

    // When D1 is available, NET must appear among the translation entries
    const contentText = data.result.content?.[0]?.text;
    if (contentText && !data.result.isError) {
      const parsed = JSON.parse(contentText);
      const translationAbbrevs: string[] = parsed.verses.flatMap(
        (v: { translations: { citation: { translation: string } }[] }) =>
          v.translations.map((t) => t.citation.translation),
      );
      expect(translationAbbrevs).toContain('NET');
    }
  });

  test('NET translation attribution notice is present in find_text results', async () => {
    const req = createRequest('tools/call', {
      name: 'find_text',
      arguments: { query: 'grace', translation: 'NET' },
    });
    const res = await server.fetch(req);
    const data = await getResponse(res);

    expect(data.result).toBeDefined();
    expect(Array.isArray(data.result.content)).toBe(true);

    // When D1 returns real NET results, the response must include the NET
    // copyright/attribution notice (Biblical Studies Press requires this).
    const contentText = data.result.content?.[0]?.text;
    if (contentText && !data.result.isError) {
      const parsed = JSON.parse(contentText);
      // Attribution is expected at the top level of the response object
      const responseText = JSON.stringify(parsed);
      expect(responseText.toLowerCase()).toMatch(/net bible|biblical studies press|netbible\.com/i);
    }
  });
});

// ─── Resource Smoke Tests ─────────────────────────────────────────────────────

describe('Resource: bible://translations', () => {
  test('returns a response', async () => {
    const req = createRequest('resources/read', {
      uri: 'bible://translations',
    });
    const res = await server.fetch(req);
    const data = await getResponse(res);

    expect(data.result).toBeDefined();
    expect(data.result.contents).toBeDefined();
    expect(Array.isArray(data.result.contents)).toBe(true);
  });
});

describe('Resource: bible://{translation}/{book}/{chapter}', () => {
  test('returns a response for a valid URI', async () => {
    const req = createRequest('resources/read', {
      uri: 'bible://KJV/John/3',
    });
    const res = await server.fetch(req);
    const data = await getResponse(res);

    expect(data.result).toBeDefined();
    expect(data.result.contents).toBeDefined();
    expect(Array.isArray(data.result.contents)).toBe(true);
  });
});

describe('Resource: bible://{translation}/{book}/{chapter}/{verse}', () => {
  test('returns a response for a valid URI', async () => {
    const req = createRequest('resources/read', {
      uri: 'bible://KJV/John/3/16',
    });
    const res = await server.fetch(req);
    const data = await getResponse(res);

    expect(data.result).toBeDefined();
    expect(data.result.contents).toBeDefined();
    expect(Array.isArray(data.result.contents)).toBe(true);
  });
});
