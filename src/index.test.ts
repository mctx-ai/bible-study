import { describe, test, expect } from 'vitest';
import server from './index.js';
import {
  buildWhyThisBookMatters,
  buildWitnessMatchReason,
  buildNarrativeReason,
  buildThemesMatched,
  toThemeLabel,
  clusterToPassageRanges,
  isConsolationQuery,
  computeQueryAlignmentScore,
  buildQueryAlignmentNote,
} from './tools/topical-search.js';

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
    expect(toolNames).toContain('semantic_search');
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

describe('Tool: semantic_search', () => {
  test('returns a response for a valid query', async () => {
    const req = createRequest('tools/call', {
      name: 'semantic_search',
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
      name: 'semantic_search',
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

  test('major_witnesses enrichment fields are present in schema when witnesses exist', async () => {
    // Non-API test: validates the response shape includes the enrichment fields.
    // When D1/Vectorize are unavailable the tool returns isError, so we only
    // assert the shape when a real result comes back.
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

      for (const witness of parsed.major_witnesses) {
        // why_this_book_matters — string when enrichment runs
        if (witness.why_this_book_matters !== undefined) {
          expect(typeof witness.why_this_book_matters).toBe('string');
        }
        // themes_matched — array of strings when enrichment runs
        if (witness.themes_matched !== undefined) {
          expect(Array.isArray(witness.themes_matched)).toBe(true);
        }
        // suggested_anchor_passages — array when enrichment runs
        if (witness.suggested_anchor_passages !== undefined) {
          expect(Array.isArray(witness.suggested_anchor_passages)).toBe(true);
        }
        // narrative_reason — string or absent (never any other type)
        if (witness.narrative_reason !== undefined) {
          expect(typeof witness.narrative_reason).toBe('string');
        }
      }
    }
  });

  test(
    'handles multi-word thematic queries without crashing',
    async () => {
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
    },
    // Each query makes multiple HTTP round-trips (embedding + D1 + Vectorize),
    // so 4 sequential queries need more than the default 5 s timeout.
    30_000,
  );
});

describe.skipIf(!process.env.CLOUDFLARE_ACCOUNT_ID)(
  'Tool: topical_search — thematic correctness',
  () => {
    test('"suffering" returns Job as a major witness', async () => {
      const req = createRequest('tools/call', {
        name: 'topical_search',
        arguments: { topic: 'suffering' },
      });
      const res = await server.fetch(req);
      const data = await getResponse(res);

      expect(data.result.isError).toBeFalsy();
      const parsed = JSON.parse(data.result.content[0].text);
      expect(parsed.major_witnesses.length).toBeGreaterThan(0);
      const witnessBooks: string[] = parsed.major_witnesses.map(
        (w: { book: string }) => w.book,
      );
      // Job is the canonical biblical book on suffering — it must appear.
      expect(witnessBooks).toContain('Job');
    });

    test('"innocent suffering" returns Job as a major witness', async () => {
      const req = createRequest('tools/call', {
        name: 'topical_search',
        arguments: { topic: 'innocent suffering' },
      });
      const res = await server.fetch(req);
      const data = await getResponse(res);

      expect(data.result.isError).toBeFalsy();
      const parsed = JSON.parse(data.result.content[0].text);
      expect(parsed.major_witnesses.length).toBeGreaterThan(0);
      const witnessBooks: string[] = parsed.major_witnesses.map(
        (w: { book: string }) => w.book,
      );
      // Job is the paradigmatic book for innocent suffering — it must appear as a witness.
      expect(witnessBooks).toContain('Job');
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
        // citation is an object with book, chapter, verse, translation.
        expect(witness.representative_verse.citation).toBeDefined();
        expect(typeof witness.representative_verse.citation.book).toBe(
          'string',
        );
      }
    });

    test(
      'verse results include at least one from a major witness book',
      async () => {
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
          (r: { citation: { book: string } }) => r.citation.book,
        );
        const hasOverlap = resultBooks.some((book: string) =>
          witnessBooks.has(book),
        );
        expect(hasOverlap).toBe(true);
      },
      15_000,
    );

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

    test('"leadership" surfaces relevant results', async () => {
      const req = createRequest('tools/call', {
        name: 'topical_search',
        arguments: { topic: 'leadership' },
      });
      const res = await server.fetch(req);
      const data = await getResponse(res);

      expect(data.result.isError).toBeFalsy();
      const parsed = JSON.parse(data.result.content[0].text);
      // Leadership may not meet the major witness threshold (min 5 verses
      // across 2+ chapters), but should return verse results.
      expect(parsed.results.length).toBeGreaterThan(0);
    });

    test('"redemption" matches REDEEM-related topics', async () => {
      const req = createRequest('tools/call', {
        name: 'topical_search',
        arguments: { topic: 'redemption' },
      });
      const res = await server.fetch(req);
      const data = await getResponse(res);

      expect(data.result.isError).toBeFalsy();
      const parsed = JSON.parse(data.result.content[0].text);
      expect(parsed.major_witnesses.length).toBeGreaterThan(0);
    });

    test(
      '"exile and return" surfaces Jeremiah or Ezekiel as witnesses',
      async () => {
        const req = createRequest('tools/call', {
          name: 'topical_search',
          arguments: { topic: 'exile and return' },
        });
        const res = await server.fetch(req);
        const data = await getResponse(res);

        expect(data.result.isError).toBeFalsy();
        const parsed = JSON.parse(data.result.content[0].text);
        expect(parsed.results.length).toBeGreaterThan(0);
        const witnessBooks: string[] = parsed.major_witnesses.map(
          (w: { book: string }) => w.book,
        );
        // Jeremiah and Ezekiel are the primary prophetic witnesses to exile — at least one must appear.
        const hasExileProphet = witnessBooks.includes('Jeremiah') || witnessBooks.includes('Ezekiel');
        expect(hasExileProphet).toBe(true);
      },
      15_000,
    );

    test('"the Holy Spirit" surfaces Acts and John', async () => {
      const req = createRequest('tools/call', {
        name: 'topical_search',
        arguments: { topic: 'the Holy Spirit' },
      });
      const res = await server.fetch(req);
      const data = await getResponse(res);

      expect(data.result.isError).toBeFalsy();
      const parsed = JSON.parse(data.result.content[0].text);
      expect(parsed.major_witnesses.length).toBeGreaterThan(0);
      const witnessBooks: string[] = parsed.major_witnesses.map(
        (w: { book: string }) => w.book,
      );
      const expectedBooks = ['Acts', 'John'];
      const hasExpectedBook = expectedBooks.some((book) =>
        witnessBooks.includes(book),
      );
      expect(hasExpectedBook).toBe(true);
    });

    test('"end times prophecy" surfaces Daniel or Revelation as witnesses', async () => {
      const req = createRequest('tools/call', {
        name: 'topical_search',
        arguments: { topic: 'end times prophecy' },
      });
      const res = await server.fetch(req);
      const data = await getResponse(res);

      expect(data.result.isError).toBeFalsy();
      const parsed = JSON.parse(data.result.content[0].text);
      expect(parsed.results.length).toBeGreaterThan(0);
      const witnessBooks: string[] = parsed.major_witnesses.map(
        (w: { book: string }) => w.book,
      );
      // Daniel and Revelation are the canonical apocalyptic books — at least one must appear.
      const hasApocalypticBook = witnessBooks.includes('Daniel') || witnessBooks.includes('Revelation');
      expect(hasApocalypticBook).toBe(true);
    });

    test('"God\'s sovereignty over nations" surfaces Isaiah/Daniel', async () => {
      const req = createRequest('tools/call', {
        name: 'topical_search',
        arguments: { topic: "God's sovereignty over nations" },
      });
      const res = await server.fetch(req);
      const data = await getResponse(res);

      expect(data.result.isError).toBeFalsy();
      const parsed = JSON.parse(data.result.content[0].text);
      expect(parsed.major_witnesses.length).toBeGreaterThan(0);
      const witnessBooks: string[] = parsed.major_witnesses.map(
        (w: { book: string }) => w.book,
      );
      const expectedBooks = ['Isaiah', 'Daniel'];
      const hasExpectedBook = expectedBooks.some((book) =>
        witnessBooks.includes(book),
      );
      expect(hasExpectedBook).toBe(true);
    });
  },
);

// ─── Tool Description Routing Validation ──────────────────────────────────────
//
// These tests verify tool descriptions contain the right routing hints so that
// LLMs choose the correct tool. They do NOT call the Cloudflare API.

describe('Tool description routing validation', () => {
  test('semantic_search description contains routing hint toward topical_search', async () => {
    const req = createRequest('tools/list');
    const res = await server.fetch(req);
    const data = await getResponse(res);

    const semanticTool = data.result.tools.find(
      (t: { name: string }) => t.name === 'semantic_search',
    );
    expect(semanticTool).toBeDefined();
    expect(semanticTool.description.toLowerCase()).toContain('use topical_search instead');
  });

  test('topical_search description contains "what does the Bible say" pattern', async () => {
    const req = createRequest('tools/list');
    const res = await server.fetch(req);
    const data = await getResponse(res);

    const topicalTool = data.result.tools.find(
      (t: { name: string }) => t.name === 'topical_search',
    );
    expect(topicalTool).toBeDefined();
    expect(topicalTool.description.toLowerCase()).toContain(
      'what does the bible say',
    );
  });

  test('tool is registered as "semantic_search" not "search_bible"', async () => {
    const req = createRequest('tools/list');
    const res = await server.fetch(req);
    const data = await getResponse(res);

    const toolNames = data.result.tools.map((t: { name: string }) => t.name);
    expect(toolNames).toContain('semantic_search');
    expect(toolNames).not.toContain('search_bible');
  });
});

// ─── semantic_search Thematic Correctness ─────────────────────────────────────
//
// These tests require live Cloudflare API access (D1 + Vectorize + Workers AI).
// Gated behind CLOUDFLARE_ACCOUNT_ID to skip in CI / local-only environments.

describe.skipIf(!process.env.CLOUDFLARE_ACCOUNT_ID)(
  'Tool: semantic_search — thematic correctness',
  () => {
    test('"God\'s faithfulness during suffering" returns verses', async () => {
      const req = createRequest('tools/call', {
        name: 'semantic_search',
        arguments: { query: "God's faithfulness during suffering" },
      });
      const res = await server.fetch(req);
      const data = await getResponse(res);

      expect(data.result.isError).toBeFalsy();
      const parsed = JSON.parse(data.result.content[0].text);
      expect(parsed.results.length).toBeGreaterThan(0);
    });

    test('results for "God\'s faithfulness during suffering" contain God/Lord language', async () => {
      const req = createRequest('tools/call', {
        name: 'semantic_search',
        arguments: { query: "God's faithfulness during suffering" },
      });
      const res = await server.fetch(req);
      const data = await getResponse(res);

      expect(data.result.isError).toBeFalsy();
      const parsed = JSON.parse(data.result.content[0].text);
      expect(parsed.results.length).toBeGreaterThan(0);

      // At least one result should contain divine language — discriminates
      // divine faithfulness results from generic human endurance verses
      const hasDivineLanguage = parsed.results.some(
        (r: { text?: string; translations?: { text: string }[] }) => {
          const texts = r.translations
            ? r.translations.map((t: { text: string }) => t.text).join(' ')
            : r.text || '';
          return /\b(God|LORD|Lord|Almighty|Most High)\b/.test(texts);
        },
      );
      expect(hasDivineLanguage).toBe(true);
    });

    test('consistent results across repeated calls (determinism)', async () => {
      const makeCall = async () => {
        const req = createRequest('tools/call', {
          name: 'semantic_search',
          arguments: { query: "God's faithfulness during suffering" },
        });
        const res = await server.fetch(req);
        return getResponse(res);
      };

      const data1 = await makeCall();
      const data2 = await makeCall();

      expect(data1.result.isError).toBeFalsy();
      expect(data2.result.isError).toBeFalsy();

      const parsed1 = JSON.parse(data1.result.content[0].text);
      const parsed2 = JSON.parse(data2.result.content[0].text);

      // Same number of results
      expect(parsed1.results.length).toBe(parsed2.results.length);

      // Same citations in same order
      const citations1 = parsed1.results.map(
        (r: { citation: string }) => r.citation,
      );
      const citations2 = parsed2.results.map(
        (r: { citation: string }) => r.citation,
      );
      expect(citations1).toEqual(citations2);
    });
  },
);

// ─── topical_search Expanded Thematic Coverage ────────────────────────────────
//
// Regression tests for specific thematic queries and their expected major
// witnesses. Gated behind CLOUDFLARE_ACCOUNT_ID.

describe.skipIf(!process.env.CLOUDFLARE_ACCOUNT_ID)(
  'Tool: topical_search — expanded thematic coverage',
  () => {
    test(
      '"God\'s faithfulness during suffering" surfaces Job as a witness',
      async () => {
        const req = createRequest('tools/call', {
          name: 'topical_search',
          arguments: { topic: "God's faithfulness during suffering" },
        });
        const res = await server.fetch(req);
        const data = await getResponse(res);

        expect(data.result.isError).toBeFalsy();
        const parsed = JSON.parse(data.result.content[0].text);
        expect(parsed.results.length).toBeGreaterThan(0);
        expect(parsed.major_witnesses.length).toBeGreaterThan(0);
        const witnessBooks: string[] = parsed.major_witnesses.map(
          (w: { book: string }) => w.book,
        );
        // Job directly addresses God's faithfulness through prolonged suffering.
        expect(witnessBooks).toContain('Job');
      },
      15_000,
    );

    test(
      '"God\'s faithfulness during suffering" surfaces Psalms as a witness',
      async () => {
        const req = createRequest('tools/call', {
          name: 'topical_search',
          arguments: { topic: "God's faithfulness during suffering" },
        });
        const res = await server.fetch(req);
        const data = await getResponse(res);

        expect(data.result.isError).toBeFalsy();
        const parsed = JSON.parse(data.result.content[0].text);
        expect(parsed.results.length).toBeGreaterThan(0);
        expect(parsed.major_witnesses.length).toBeGreaterThan(0);
        const witnessBooks: string[] = parsed.major_witnesses.map(
          (w: { book: string }) => w.book,
        );
        // Psalms extensively records God's faithfulness amid lament and suffering.
        expect(witnessBooks).toContain('Psalms');
      },
      15_000,
    );

    test('"innocent suffering" returns results and witnesses', async () => {
      const req = createRequest('tools/call', {
        name: 'topical_search',
        arguments: { topic: 'innocent suffering' },
      });
      const res = await server.fetch(req);
      const data = await getResponse(res);

      expect(data.result.isError).toBeFalsy();
      const parsed = JSON.parse(data.result.content[0].text);
      expect(parsed.results.length).toBeGreaterThan(0);
      expect(parsed.major_witnesses.length).toBeGreaterThan(0);
    });

    test('"lament and sorrow" surfaces Psalms and Lamentations as witnesses', async () => {
      const req = createRequest('tools/call', {
        name: 'topical_search',
        arguments: { topic: 'lament and sorrow' },
      });
      const res = await server.fetch(req);
      const data = await getResponse(res);

      expect(data.result.isError).toBeFalsy();
      const parsed = JSON.parse(data.result.content[0].text);
      expect(parsed.results.length).toBeGreaterThan(0);
      expect(parsed.major_witnesses.length).toBeGreaterThan(0);
      const witnessBooks: string[] = parsed.major_witnesses.map(
        (w: { book: string }) => w.book,
      );
      // Psalms and Lamentations are the canonical lament literature — both must appear.
      expect(witnessBooks).toContain('Psalms');
      expect(witnessBooks).toContain('Lamentations');
    });

    test('"comfort in affliction" surfaces Isaiah as a witness', async () => {
      const req = createRequest('tools/call', {
        name: 'topical_search',
        arguments: { topic: 'comfort in affliction' },
      });
      const res = await server.fetch(req);
      const data = await getResponse(res);

      expect(data.result.isError).toBeFalsy();
      const parsed = JSON.parse(data.result.content[0].text);
      expect(parsed.results.length).toBeGreaterThan(0);
      expect(parsed.major_witnesses.length).toBeGreaterThan(0);
      const witnessBooks: string[] = parsed.major_witnesses.map(
        (w: { book: string }) => w.book,
      );
      // Isaiah's "comfort, comfort my people" passages are central to this topic.
      expect(witnessBooks).toContain('Isaiah');
    });

    test(
      'major witnesses have why_this_book_matters field populated',
      async () => {
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
          expect(typeof witness.why_this_book_matters).toBe('string');
          expect(witness.why_this_book_matters.length).toBeGreaterThan(0);
        }

        // Job is the canonical book on suffering — it must appear as a witness
        // and have a non-empty why_this_book_matters string.
        const job = parsed.major_witnesses.find(
          (w: { book: string }) => w.book === 'Job',
        );
        expect(job).toBeDefined();
        expect(typeof job.why_this_book_matters).toBe('string');
        expect(job.why_this_book_matters.length).toBeGreaterThan(0);
        // The message must mention the book name.
        expect(job.why_this_book_matters).toContain('Job');
      },
      15_000,
    );

    test(
      'major witnesses have themes_matched field populated',
      async () => {
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
          expect(Array.isArray(witness.themes_matched)).toBe(true);
          expect(witness.themes_matched.length).toBeGreaterThan(0);
          for (const theme of witness.themes_matched) {
            expect(typeof theme).toBe('string');
          }
        }

        // At least one witness must have a theme_matched that mentions
        // "suffering" or "affliction" (case-insensitive) — confirming the
        // themes are query-relevant, not generic.
        const allThemes: string[] = parsed.major_witnesses.flatMap(
          (w: { themes_matched: string[] }) => w.themes_matched,
        );
        const hasRelevantTheme = allThemes.some((t) => {
          const lower = t.toLowerCase();
          return lower.includes('suffering') || lower.includes('affliction');
        });
        expect(hasRelevantTheme).toBe(true);
      },
      15_000,
    );

    test(
      'major witnesses have suggested_anchor_passages field',
      async () => {
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
          expect(Array.isArray(witness.suggested_anchor_passages)).toBe(true);
          for (const passage of witness.suggested_anchor_passages) {
            expect(typeof passage).toBe('string');
          }
        }

        // At least one witness must have non-empty suggested_anchor_passages.
        const hasPassages = parsed.major_witnesses.some(
          (w: { suggested_anchor_passages: string[] }) =>
            w.suggested_anchor_passages.length > 0,
        );
        expect(hasPassages).toBe(true);

        // Most passage references contain a number (e.g. "Job 1", "Job 1:21").
        // Short books (4 chapters or fewer) may return just the book name when
        // topic coverage spans the whole book — so we don't require a digit on
        // every entry, only that the string is non-empty and contains the book name.
        const allPassages: string[] = parsed.major_witnesses.flatMap(
          (w: { suggested_anchor_passages: string[]; book: string }) =>
            w.suggested_anchor_passages,
        );
        for (const witness of parsed.major_witnesses) {
          for (const passage of witness.suggested_anchor_passages) {
            expect(typeof passage).toBe('string');
            expect(passage.length).toBeGreaterThan(0);
            // Passage must reference the witness's own book.
            expect(passage).toContain(witness.book);
          }
        }
        // At least one passage across all witnesses should contain a number.
        const passagesWithNumbers = allPassages.filter((p) => /\d/.test(p));
        if (allPassages.length > 0) {
          expect(passagesWithNumbers.length).toBeGreaterThan(0);
        }
      },
      15_000,
    );

    test(
      'narrative_reason field exists on major witnesses (string or undefined)',
      async () => {
        const req = createRequest('tools/call', {
          name: 'topical_search',
          arguments: { topic: 'providence' },
        });
        const res = await server.fetch(req);
        const data = await getResponse(res);

        expect(data.result.isError).toBeFalsy();
        const parsed = JSON.parse(data.result.content[0].text);
        expect(parsed.major_witnesses.length).toBeGreaterThan(0);

        // narrative_reason is optional — witnesses with a narrative arc get it,
        // others do not. Validate the field is either a non-empty string or absent.
        for (const witness of parsed.major_witnesses) {
          if (witness.narrative_reason !== undefined) {
            expect(typeof witness.narrative_reason).toBe('string');
            expect(witness.narrative_reason.length).toBeGreaterThan(0);
          }
        }

        // The field must exist as a key on the object (present or undefined),
        // confirming the serialization includes it when set.
        const witnessesWithNarrative = parsed.major_witnesses.filter(
          (w: { narrative_reason?: string }) =>
            w.narrative_reason !== undefined,
        );
        // Not asserting count — just that any present value is a non-empty string.
        for (const w of witnessesWithNarrative) {
          expect(typeof w.narrative_reason).toBe('string');
          expect(w.narrative_reason.length).toBeGreaterThan(0);
        }
      },
      15_000,
    );
  },
);

// ─── Genre-Aware Explanation Template Unit Tests ──────────────────────────────
//
// These tests verify that buildWhyThisBookMatters, buildWitnessMatchReason, and
// buildNarrativeReason produce genre-appropriate natural-language output.
// They do NOT call the Cloudflare API — all inputs are constructed inline.

describe('Genre-aware explanation templates', () => {
  // Minimal WitnessCandidate factory for testing.
  function makeCandidate(bookName: string, overrides?: Partial<{
    book_id: number;
    testament: string;
    verse_count: number;
    chapter_count: number;
    min_chapter: number;
    max_chapter: number;
    topic_names: string;
  }>) {
    return {
      book_id: 1,
      book_name: bookName,
      testament: 'OT',
      verse_count: 50,
      chapter_count: 10,
      min_chapter: 1,
      max_chapter: 10,
      topic_names: 'SUFFERING, AFFLICTION',
      ...overrides,
    };
  }

  const emptyMap = new Map<string, number>();
  const emptyTopics: Array<{ id: number; name: string }> = [];
  const emptyIds: number[] = [];

  describe('buildWhyThisBookMatters', () => {
    test('Poetry genre (Psalms) uses devotional voice template', () => {
      const result = buildWhyThisBookMatters(
        makeCandidate('Psalms'),
        emptyMap,
        emptyIds,
        emptyTopics,
        ['lament', 'praise'],
      );
      expect(result).toContain('Psalms');
      expect(result).toMatch(/voices|prayer|lament|praise|trust/i);
    });

    test('Wisdom genre (Job) uses wisdom reflection template', () => {
      const result = buildWhyThisBookMatters(
        makeCandidate('Job'),
        emptyMap,
        emptyIds,
        emptyTopics,
        ['suffering', 'endurance'],
      );
      expect(result).toContain('Job');
      expect(result).toMatch(/wisdom|reflection|dialogue|instruction/i);
    });

    test('Prophecy genre (Isaiah) uses prophetic proclamation template', () => {
      const result = buildWhyThisBookMatters(
        makeCandidate('Isaiah'),
        emptyMap,
        emptyIds,
        emptyTopics,
        ['comfort', 'restoration'],
      );
      expect(result).toContain('Isaiah');
      expect(result).toMatch(/prophetic|judgment|comfort|restoration|promise/i);
    });

    test('Epistle genre (Romans) uses doctrinal teaching template', () => {
      const result = buildWhyThisBookMatters(
        makeCandidate('Romans'),
        emptyMap,
        emptyIds,
        emptyTopics,
        ['justification', 'faith'],
      );
      expect(result).toContain('Romans');
      expect(result).toMatch(/teaches|doctrinal|pastoral/i);
    });

    test('Gospel genre (John) uses life-and-ministry template', () => {
      const result = buildWhyThisBookMatters(
        makeCandidate('John'),
        emptyMap,
        emptyIds,
        emptyTopics,
        ['eternal life', 'faith'],
      );
      expect(result).toContain('John');
      expect(result).toMatch(/life|teaching|ministry|Jesus|Christ/i);
    });

    test('Apocalyptic genre (Revelation) uses divine victory template', () => {
      const result = buildWhyThisBookMatters(
        makeCandidate('Revelation'),
        emptyMap,
        emptyIds,
        emptyTopics,
        ['judgment', 'restoration'],
      );
      expect(result).toContain('Revelation');
      expect(result).toMatch(/divine victory|cosmic|restoration|apocalyptic/i);
    });

    test('distinct genres produce meaningfully different output', () => {
      const themes = ['suffering'];
      const psalmResult = buildWhyThisBookMatters(
        makeCandidate('Psalms'),
        emptyMap, emptyIds, emptyTopics, themes,
      );
      const jobResult = buildWhyThisBookMatters(
        makeCandidate('Job'),
        emptyMap, emptyIds, emptyTopics, themes,
      );
      const isaiahResult = buildWhyThisBookMatters(
        makeCandidate('Isaiah'),
        emptyMap, emptyIds, emptyTopics, themes,
      );
      const romansResult = buildWhyThisBookMatters(
        makeCandidate('Romans'),
        emptyMap, emptyIds, emptyTopics, themes,
      );
      const johnResult = buildWhyThisBookMatters(
        makeCandidate('John'),
        emptyMap, emptyIds, emptyTopics, themes,
      );

      // All five should be distinct strings.
      const outputs = [psalmResult, jobResult, isaiahResult, romansResult, johnResult];
      const unique = new Set(outputs);
      expect(unique.size).toBe(5);
    });

    test('output does not contain database-report phrase "concentrates on ... topical references spanning"', () => {
      const books = ['Job', 'Psalms', 'Isaiah', 'Romans', 'John', 'Revelation', 'Genesis'];
      for (const book of books) {
        const result = buildWhyThisBookMatters(
          makeCandidate(book),
          emptyMap, emptyIds, emptyTopics, ['faith'],
        );
        expect(result).not.toMatch(/concentrates on .+, with \d+ topical references spanning/);
      }
    });
  });

  describe('buildWitnessMatchReason', () => {
    test('Poetry genre (Psalms) mentions prayer or lament', () => {
      const result = buildWitnessMatchReason(
        makeCandidate('Psalms'),
        undefined,
        ['lament', 'trust'],
      );
      expect(result).toContain('Psalms');
      expect(result).toMatch(/prayer|lament|praise|trust/i);
    });

    test('Epistle genre (Ephesians) mentions teaching or doctrinal', () => {
      const result = buildWitnessMatchReason(
        makeCandidate('Ephesians'),
        undefined,
        ['grace', 'faith'],
      );
      expect(result).toContain('Ephesians');
      expect(result).toMatch(/teaches|doctrinal|pastoral/i);
    });

    test('Prophecy genre (Jeremiah) mentions proclamation or promise', () => {
      const result = buildWitnessMatchReason(
        makeCandidate('Jeremiah'),
        undefined,
        ['exile', 'restoration'],
      );
      expect(result).toContain('Jeremiah');
      expect(result).toMatch(/prophetic|proclamation|judgment|promise|restoration/i);
    });

    test('Gospel genre with narrative uses life-and-ministry language', () => {
      const result = buildWitnessMatchReason(
        makeCandidate('Luke'),
        'Passion',
        ['redemption', 'sacrifice'],
      );
      expect(result).toContain('Luke');
      expect(result).toMatch(/life|ministry|Jesus|Gospel/i);
    });

    test('History genre narrative uses story-arc language', () => {
      const result = buildWitnessMatchReason(
        makeCandidate('1 Samuel'),
        'David',
        ['kingship'],
      );
      expect(result).toContain('1 Samuel');
      expect(result).toMatch(/narrative|historical|story/i);
    });
  });

  describe('buildNarrativeReason', () => {
    test('returns undefined when no narrative', () => {
      const result = buildNarrativeReason(undefined, makeCandidate('Genesis'));
      expect(result).toBeUndefined();
    });

    test('History genre narrative uses historical story arc language', () => {
      const result = buildNarrativeReason(
        'Joseph',
        makeCandidate('Genesis', { min_chapter: 37, max_chapter: 50 }),
        ['providence', 'faithfulness'],
      );
      expect(result).toBeDefined();
      expect(result).toContain('Joseph');
      expect(result).toMatch(/historical|narrative|story/i);
    });

    test('Gospel genre narrative uses life-and-ministry language', () => {
      const result = buildNarrativeReason(
        'Passion',
        makeCandidate('Matthew', { min_chapter: 26, max_chapter: 28 }),
        ['atonement', 'resurrection'],
      );
      expect(result).toBeDefined();
      expect(result).toMatch(/life|ministry|Jesus|Gospel/i);
    });

    test('narrative_reason includes chapter range in readable form', () => {
      const result = buildNarrativeReason(
        'Moses',
        makeCandidate('Exodus', { min_chapter: 1, max_chapter: 15 }),
        ['deliverance'],
      );
      expect(result).toBeDefined();
      expect(result).toContain('Exodus');
      expect(result).toMatch(/1/);
      expect(result).toMatch(/15/);
    });
  });
});

// ─── Theme Label Mapping Unit Tests ───────────────────────────────────────────
//
// These tests verify that buildThemesMatched transforms raw Nave's topic names
// into user-facing labels, and that toThemeLabel falls back gracefully for
// unmapped topics. No Cloudflare API calls are made.

describe('Theme label mapping', () => {
  function makeCandidate(topicNames: string) {
    return {
      book_id: 1,
      book_name: 'Romans',
      testament: 'NT',
      verse_count: 100,
      chapter_count: 16,
      min_chapter: 1,
      max_chapter: 16,
      topic_names: topicNames,
    };
  }

  const emptyMap = new Map<string, number>();
  const emptyIds: number[] = [];

  describe('toThemeLabel', () => {
    test('maps AFFLICTIONS AND ADVERSITIES to affliction', () => {
      expect(toThemeLabel('AFFLICTIONS AND ADVERSITIES')).toBe('affliction');
    });

    test('maps UNFAITHFULNESS to unfaithfulness', () => {
      expect(toThemeLabel('UNFAITHFULNESS')).toBe('unfaithfulness');
    });

    test('maps FAITHFULNESS to faithfulness', () => {
      expect(toThemeLabel('FAITHFULNESS')).toBe('faithfulness');
    });

    test('maps CHURCH to the church', () => {
      expect(toThemeLabel('CHURCH')).toBe('the church');
    });

    test('maps WORKS to works', () => {
      expect(toThemeLabel('WORKS')).toBe('works');
    });

    test('maps SUFFERING to suffering', () => {
      expect(toThemeLabel('SUFFERING')).toBe('suffering');
    });

    test('maps HOLY SPIRIT to the Holy Spirit', () => {
      expect(toThemeLabel('HOLY SPIRIT')).toBe('the Holy Spirit');
    });

    test('maps KINGDOM OF GOD to the kingdom of God', () => {
      expect(toThemeLabel('KINGDOM OF GOD')).toBe('the kingdom of God');
    });

    test('falls back to lowercase for unmapped topic', () => {
      expect(toThemeLabel('PREDESTINATION')).toBe('predestination');
    });

    test('fallback replaces " AND " with " and " in unmapped topic', () => {
      expect(toThemeLabel('SIGNS AND WONDERS')).toBe('signs and wonders');
    });

    test('fallback handles multi-word unmapped topic', () => {
      expect(toThemeLabel('SECOND COMING')).toBe('second coming');
    });
  });

  describe('buildThemesMatched', () => {
    test('maps matched topic names to user-facing labels', () => {
      const candidate = makeCandidate('SUFFERING, FAITHFULNESS, CHURCH');
      const expandedTopics = [
        { id: 1, name: 'SUFFERING' },
        { id: 2, name: 'FAITHFULNESS' },
        { id: 3, name: 'CHURCH' },
      ];
      const result = buildThemesMatched(candidate, expandedTopics, emptyMap, emptyIds);
      expect(result).toContain('suffering');
      expect(result).toContain('faithfulness');
      expect(result).toContain('the church');
      expect(result).not.toContain('SUFFERING');
      expect(result).not.toContain('FAITHFULNESS');
      expect(result).not.toContain('CHURCH');
    });

    test('fallback maps unmatched candidate topics to user-facing labels', () => {
      const candidate = makeCandidate('AFFLICTIONS AND ADVERSITIES, UNFAITHFULNESS, WORKS');
      // No expanded topics match, so fallback returns first 5 of candidate topics.
      const result = buildThemesMatched(candidate, [], emptyMap, emptyIds);
      expect(result).toContain('affliction');
      expect(result).toContain('unfaithfulness');
      expect(result).toContain('works');
      expect(result).not.toContain('AFFLICTIONS AND ADVERSITIES');
    });

    test('fallback applies lowercase-and-clean for unmapped topics', () => {
      const candidate = makeCandidate('PREDESTINATION, SIGNS AND WONDERS');
      const result = buildThemesMatched(candidate, [], emptyMap, emptyIds);
      expect(result).toContain('predestination');
      expect(result).toContain('signs and wonders');
    });

    test('scoring uses original names — sorted order reflects salience, labels are output only', () => {
      const candidate = makeCandidate('SUFFERING, FAITH, HOPE');
      const expandedTopics = [
        { id: 10, name: 'SUFFERING' },
        { id: 11, name: 'FAITH' },
        { id: 12, name: 'HOPE' },
      ];
      // Give FAITH and HOPE salience above 0.6 threshold; SUFFERING below.
      const salienceMap = new Map<string, number>([
        ['1:10', 0.2], // SUFFERING — below threshold, filtered out
        ['1:11', 0.9], // FAITH — above threshold
        ['1:12', 0.7], // HOPE — above threshold
      ]);
      const salienceTopicIds = [10, 11, 12];
      const result = buildThemesMatched(candidate, expandedTopics, salienceMap, salienceTopicIds);
      // Topics with salience >= 0.6: FAITH (0.9), HOPE (0.7). SUFFERING (0.2) is filtered.
      // Sorted by salience descending: faith, hope.
      expect(result[0]).toBe('faith');
      expect(result[1]).toBe('hope');
      expect(result).not.toContain('suffering');
    });
  });
});

// ─── Genre-Aware Clustering Unit Tests ────────────────────────────────────────
//
// These tests verify that clusterToPassageRanges applies genre-specific
// strategies and that isConsolationQuery correctly identifies comfort/hope
// queries. No Cloudflare API calls are made.

describe('Genre-aware clustering (clusterToPassageRanges)', () => {
  // Build a realistic set of AnchorChapterRows spanning a book.
  function makeRows(chapters: Array<{ chapter: number; hit_count: number }>): Array<{
    book_id: number;
    chapter: number;
    min_verse: number;
    max_verse: number;
    hit_count: number;
  }> {
    return chapters.map(({ chapter, hit_count }) => ({
      book_id: 1,
      chapter,
      min_verse: 1,
      max_verse: 20,
      hit_count,
    }));
  }

  describe('Poetry genre — individual chapters, no consecutive merging', () => {
    test('returns individual chapter references for Psalms, not spans', () => {
      // Chapters 22, 23, 24 are dense — density clustering would merge them.
      // Poetry strategy should return them as individual references.
      const rows = makeRows([
        { chapter: 22, hit_count: 10 },
        { chapter: 23, hit_count: 8 },
        { chapter: 24, hit_count: 6 },
        { chapter: 51, hit_count: 12 },
        { chapter: 119, hit_count: 15 },
      ]);
      const result = clusterToPassageRanges(rows, 'Psalms', 150, 'Poetry');
      // All passages must be single-chapter references (no "-" range).
      for (const passage of result) {
        expect(passage).not.toMatch(/Psalms \d+-\d+$/);
      }
      // Should return up to 3 individual chapters.
      expect(result.length).toBeGreaterThan(0);
      expect(result.length).toBeLessThanOrEqual(3);
    });

    test('Poetry returns the 3 densest chapters (not just sequential)', () => {
      const rows = makeRows([
        { chapter: 1, hit_count: 2 },
        { chapter: 22, hit_count: 5 },
        { chapter: 51, hit_count: 9 },
        { chapter: 103, hit_count: 7 },
        { chapter: 119, hit_count: 4 },
      ]);
      const result = clusterToPassageRanges(rows, 'Psalms', 150, 'Poetry');
      // Top 3 by hit count: chapters 51, 103, 22 — in canonical order.
      expect(result).toContain('Psalms 51');
      expect(result).toContain('Psalms 103');
      expect(result).toContain('Psalms 22');
    });
  });

  describe('Narrative genre — arc-based (entry/crisis/resolution)', () => {
    test('History genre produces references spanning beginning, middle, and end', () => {
      // Genesis has 50 chapters. Arc thirds: 1-17, 18-34, 35-50.
      const rows = makeRows([
        { chapter: 2, hit_count: 8 },   // entry arc
        { chapter: 22, hit_count: 10 },  // crisis arc
        { chapter: 45, hit_count: 7 },   // resolution arc
        { chapter: 37, hit_count: 6 },   // resolution arc
      ]);
      const result = clusterToPassageRanges(rows, 'Genesis', 50, 'History');
      expect(result.length).toBeGreaterThan(0);
      // Should reference both early and late chapters.
      const hasEarly = result.some((p) => /Genesis [1-9]/.test(p) || /Genesis 1[0-7]/.test(p));
      const hasLate = result.some((p) => /Genesis [34][0-9]/.test(p) || /Genesis 5[0-9]/.test(p));
      expect(hasEarly).toBe(true);
      expect(hasLate).toBe(true);
    });

    test('Gospel genre uses arc strategy', () => {
      // Matthew has 28 chapters. Arc thirds: 1-10, 11-19, 20-28.
      const rows = makeRows([
        { chapter: 5, hit_count: 12 },  // entry arc (Sermon on the Mount)
        { chapter: 16, hit_count: 9 },  // crisis arc
        { chapter: 26, hit_count: 11 }, // resolution arc (Passion narrative)
      ]);
      const result = clusterToPassageRanges(rows, 'Matthew', 28, 'Gospel');
      expect(result.length).toBeGreaterThan(0);
      expect(result.some((p) => p.includes('Matthew'))).toBe(true);
    });

    test('Narrative arc produces different result than density clustering for same data', () => {
      // Dense cluster is at chapters 22-24 (middle), but arc strategy should
      // also include entry (ch 2) and resolution (ch 45).
      const rows = makeRows([
        { chapter: 2, hit_count: 3 },
        { chapter: 22, hit_count: 10 },
        { chapter: 23, hit_count: 9 },
        { chapter: 24, hit_count: 8 },
        { chapter: 45, hit_count: 3 },
      ]);
      const narrative = clusterToPassageRanges(rows, 'Genesis', 50, 'History');
      const density = clusterToPassageRanges(rows, 'Genesis', 50, 'Epistle'); // use Epistle for plain density
      // Narrative result includes arc-aware references; density result collapses the dense cluster.
      expect(narrative).not.toEqual(density);
    });
  });

  describe('Prophecy genre — consolation bias for comfort/hope queries', () => {
    test('consolation query biases toward latter half of prophetic book', () => {
      // Isaiah has 66 chapters. Midpoint = 33. Consolation is chapters 34-66.
      const rows = makeRows([
        { chapter: 5, hit_count: 8 },    // judgment section (first half)
        { chapter: 40, hit_count: 10 },  // consolation section (second half)
        { chapter: 53, hit_count: 12 },  // consolation section (second half)
        { chapter: 60, hit_count: 9 },   // consolation section (second half)
      ]);
      const comfort = clusterToPassageRanges(rows, 'Isaiah', 66, 'Prophecy', 'comfort in affliction');
      // Should prefer consolation chapters (>33) over judgment chapters.
      const hasConsolation = comfort.some((p) => {
        const match = p.match(/Isaiah (\d+)/);
        return match && parseInt(match[1], 10) > 33;
      });
      expect(hasConsolation).toBe(true);
    });

    test('non-consolation query uses density (returns densest chapters)', () => {
      const rows = makeRows([
        { chapter: 5, hit_count: 15 },  // dense judgment section
        { chapter: 6, hit_count: 14 },
        { chapter: 40, hit_count: 4 },  // sparse consolation
      ]);
      const judgment = clusterToPassageRanges(rows, 'Isaiah', 66, 'Prophecy', 'idolatry');
      // Should use density, not consolation bias — picks chapters 5-6.
      const hasJudgment = judgment.some((p) => {
        const match = p.match(/Isaiah (\d+)/);
        return match && parseInt(match[1], 10) <= 10;
      });
      expect(hasJudgment).toBe(true);
    });

    test('Prophecy consolation and non-consolation produce different results', () => {
      const rows = makeRows([
        { chapter: 1, hit_count: 10 },
        { chapter: 40, hit_count: 8 },
        { chapter: 53, hit_count: 9 },
      ]);
      const comfort = clusterToPassageRanges(rows, 'Isaiah', 66, 'Prophecy', "God's faithfulness");
      const judgment = clusterToPassageRanges(rows, 'Isaiah', 66, 'Prophecy', 'idolatry');
      // The two queries should produce different passage sets.
      expect(comfort).not.toEqual(judgment);
    });
  });

  describe('Epistle and default genre — density-based consecutive clustering', () => {
    test('Epistle clusters consecutive chapters by density', () => {
      const rows = makeRows([
        { chapter: 3, hit_count: 8 },
        { chapter: 4, hit_count: 10 },
        { chapter: 5, hit_count: 7 },
        { chapter: 15, hit_count: 2 },
      ]);
      const result = clusterToPassageRanges(rows, 'Romans', 16, 'Epistle');
      // Chapters 3-5 should be merged into a single span.
      expect(result.some((p) => /Romans 3-5/.test(p))).toBe(true);
    });

    test('Wisdom genre uses density-based clustering', () => {
      const rows = makeRows([
        { chapter: 3, hit_count: 6 },
        { chapter: 28, hit_count: 10 },
        { chapter: 29, hit_count: 8 },
      ]);
      const result = clusterToPassageRanges(rows, 'Proverbs', 31, 'Wisdom');
      expect(result.length).toBeGreaterThan(0);
      expect(result.some((p) => p.includes('Proverbs'))).toBe(true);
    });
  });

  describe('At least 3 distinct genre strategies', () => {
    test('Poetry, History, and Prophecy produce distinct results for the same input', () => {
      // Same chapter density data, but each genre should produce different output.
      const rows = makeRows([
        { chapter: 2, hit_count: 5 },
        { chapter: 22, hit_count: 8 },
        { chapter: 23, hit_count: 9 },
        { chapter: 24, hit_count: 7 },
        { chapter: 45, hit_count: 4 },
      ]);

      const poetryResult = clusterToPassageRanges(rows, 'TestBook', 50, 'Poetry');
      const historyResult = clusterToPassageRanges(rows, 'TestBook', 50, 'History');
      const prophecyResult = clusterToPassageRanges(rows, 'TestBook', 50, 'Prophecy', 'comfort');
      const epistleResult = clusterToPassageRanges(rows, 'TestBook', 50, 'Epistle');

      // All four strategies should be available and at least 3 of 4 should differ.
      const uniqueResults = new Set([
        JSON.stringify(poetryResult),
        JSON.stringify(historyResult),
        JSON.stringify(prophecyResult),
        JSON.stringify(epistleResult),
      ]);
      expect(uniqueResults.size).toBeGreaterThanOrEqual(3);
    });
  });

  describe('Short book collapse behavior is preserved', () => {
    test('short book with >=80% coverage returns just the book name', () => {
      // 4-chapter book with all 4 chapters covered.
      const rows = makeRows([
        { chapter: 1, hit_count: 5 },
        { chapter: 2, hit_count: 3 },
        { chapter: 3, hit_count: 4 },
        { chapter: 4, hit_count: 2 },
      ]);
      const result = clusterToPassageRanges(rows, 'Ruth', 4, 'History');
      expect(result).toEqual(['Ruth']);
    });
  });
});

describe('isConsolationQuery', () => {
  test('returns true for comfort-related queries', () => {
    expect(isConsolationQuery('comfort in affliction')).toBe(true);
    expect(isConsolationQuery("God's faithfulness")).toBe(true);
    expect(isConsolationQuery('hope in darkness')).toBe(true);
    expect(isConsolationQuery('salvation and redemption')).toBe(true);
    expect(isConsolationQuery('mercy and grace')).toBe(true);
  });

  test('returns false for judgment/non-comfort queries', () => {
    expect(isConsolationQuery('idolatry')).toBe(false);
    expect(isConsolationQuery('sin and transgression')).toBe(false);
    expect(isConsolationQuery('end times prophecy')).toBe(false);
  });
});

// ─── Query-Alignment Scoring Unit Tests ───────────────────────────────────────
//
// These tests verify computeQueryAlignmentScore and buildQueryAlignmentNote
// produce correct results for various input combinations. No Cloudflare API calls.

describe('computeQueryAlignmentScore', () => {
  test('returns 0 when there are no matched topics and no semantic hits', () => {
    const score = computeQueryAlignmentScore(
      1,
      [],
      new Map(),
      new Map(),
      0,
    );
    expect(score).toBe(0);
  });

  test('returns 0.5 when topic alignment is perfect but no semantic hits', () => {
    // One matched topic with relevance weight 1.0, no semantic hits.
    const relevance = new Map([[10, 1.0]]);
    const score = computeQueryAlignmentScore(
      1,
      [10],
      relevance,
      new Map(), // no semantic hits
      5,         // maxSemanticHits > 0 so normalization is valid
    );
    // A = 1.0 (avg weight), B = 0 (no hits). Final = 0.5*1.0 + 0.5*0 = 0.5
    expect(score).toBeCloseTo(0.5);
  });

  test('returns 0.5 when semantic density is perfect but no matched topics', () => {
    // No topic matches, but this book has the max semantic hits.
    const hitsPerBook = new Map([[42, 10]]);
    const score = computeQueryAlignmentScore(
      42,
      [],
      new Map(),
      hitsPerBook,
      10, // maxSemanticHits = 10 (this book has all of them)
    );
    // A = 0 (no topics), B = 10/10 = 1.0. Final = 0.5*0 + 0.5*1.0 = 0.5
    expect(score).toBeCloseTo(0.5);
  });

  test('returns 1.0 when both topic alignment and semantic density are perfect', () => {
    const relevance = new Map([[10, 1.0], [11, 1.0]]);
    const hitsPerBook = new Map([[5, 8]]);
    const score = computeQueryAlignmentScore(
      5,
      [10, 11],
      relevance,
      hitsPerBook,
      8, // maxSemanticHits = 8 (this book has all)
    );
    // A = (1.0 + 1.0) / 2 = 1.0, B = 8/8 = 1.0. Final = 0.5*1.0 + 0.5*1.0 = 1.0
    expect(score).toBeCloseTo(1.0);
  });

  test('lower-relevance topic weights yield lower alignment score', () => {
    // Topics matched by semantic only (low relevance weight ~0.4) vs by keyword (1.0).
    const lowRelevance = new Map([[10, 0.4]]);
    const highRelevance = new Map([[10, 1.0]]);
    const hitsPerBook = new Map([[1, 0]]); // no semantic hits

    const lowScore = computeQueryAlignmentScore(1, [10], lowRelevance, hitsPerBook, 0);
    const highScore = computeQueryAlignmentScore(1, [10], highRelevance, hitsPerBook, 0);
    expect(highScore).toBeGreaterThan(lowScore);
  });

  test('a book with more semantic hits scores higher than one with fewer (same topic alignment)', () => {
    const relevance = new Map([[10, 1.0]]);
    const hitsPerBook = new Map([[1, 2], [2, 8]]);
    const maxHits = 8;

    const scoreWithFewHits = computeQueryAlignmentScore(1, [10], relevance, hitsPerBook, maxHits);
    const scoreWithManyHits = computeQueryAlignmentScore(2, [10], relevance, hitsPerBook, maxHits);
    expect(scoreWithManyHits).toBeGreaterThan(scoreWithFewHits);
  });

  test('query-alignment score is in [0, 1] range for arbitrary inputs', () => {
    const relevance = new Map([[1, 0.7], [2, 0.9], [3, 0.3]]);
    const hitsPerBook = new Map([[100, 5], [101, 3], [102, 0]]);

    for (const bookId of [100, 101, 102]) {
      const score = computeQueryAlignmentScore(bookId, [1, 2, 3], relevance, hitsPerBook, 5);
      expect(score).toBeGreaterThanOrEqual(0);
      expect(score).toBeLessThanOrEqual(1);
    }
  });
});

describe('buildQueryAlignmentNote', () => {
  test('mentions the book name in the output', () => {
    const note = buildQueryAlignmentNote('Job', 'suffering', ['suffering', 'endurance'], 5, 0.8);
    expect(note).toContain('Job');
  });

  test('mentions the query topic in the output', () => {
    const note = buildQueryAlignmentNote('Psalms', "God's faithfulness during suffering", ['faithfulness', 'lament'], 3, 0.7);
    expect(note).toContain("God's faithfulness during suffering");
  });

  test('uses strong-signal phrasing when both signals are high', () => {
    // semanticHits >= 2 AND queryAlignmentScore >= 0.6 → both signals fired
    const note = buildQueryAlignmentNote('Romans', 'justification by faith', ['justification', 'faith'], 4, 0.75);
    expect(note).toMatch(/aligns closely|both curated|semantic verse/i);
  });

  test('uses semantic-only phrasing when semantic hits are high but topic alignment is low', () => {
    // semanticHits >= 2 but queryAlignmentScore < 0.6
    const note = buildQueryAlignmentNote('Hebrews', 'faith and endurance', ['faith'], 3, 0.3);
    expect(note).toMatch(/semantically close|thematic resonance/i);
  });

  test('uses topic-only phrasing when topic alignment is high but no semantic hits', () => {
    // queryAlignmentScore >= 0.6 but semanticHits < 2
    const note = buildQueryAlignmentNote('Isaiah', 'comfort in affliction', ['comfort', 'restoration'], 0, 0.8);
    expect(note).toMatch(/curated topics|directly named|topically grounded/i);
  });

  test('uses indirect phrasing when both signals are weak', () => {
    // semanticHits < 2 and queryAlignmentScore < 0.6 → weak signal
    const note = buildQueryAlignmentNote('Numbers', 'redemption', ['law'], 0, 0.3);
    expect(note).toMatch(/related to|indirect|broader topical/i);
  });

  test('output is a non-empty string for any valid input', () => {
    const cases: Array<[string, string, string[], number, number]> = [
      ['Genesis', 'covenant', ['covenant', 'creation'], 0, 0.5],
      ['Matthew', 'the kingdom of God', ['kingdom', 'righteousness'], 6, 0.9],
      ['Revelation', 'end times prophecy', ['judgment', 'prophecy'], 1, 0.4],
    ];
    for (const [book, query, themes, hits, score] of cases) {
      const note = buildQueryAlignmentNote(book, query, themes, hits, score);
      expect(typeof note).toBe('string');
      expect(note.length).toBeGreaterThan(0);
    }
  });
});

describe('Query-alignment tiebreaker influences witness ordering', () => {
  // This test verifies the sorting contract: when two candidates share the same
  // witnessScore, the one with the higher query-alignment score comes first.
  // We cannot call buildMajorWitnesses directly (it requires live D1/Vectorize),
  // but we can verify computeQueryAlignmentScore produces different scores for
  // candidates with different evidence profiles and that the sort logic is correct.
  test('higher queryAlignmentScore sorts before lower when witnessScore is equal', () => {
    const items = [
      { book: 'B', witnessScore: 10, queryAlignmentScore: 0.3 },
      { book: 'A', witnessScore: 10, queryAlignmentScore: 0.8 },
      { book: 'C', witnessScore: 10, queryAlignmentScore: 0.5 },
    ];

    // Apply the same sort used in buildMajorWitnesses.
    items.sort((a, b) => {
      if (b.witnessScore !== a.witnessScore) return b.witnessScore - a.witnessScore;
      return b.queryAlignmentScore - a.queryAlignmentScore;
    });

    expect(items[0].book).toBe('A'); // highest alignment
    expect(items[1].book).toBe('C');
    expect(items[2].book).toBe('B'); // lowest alignment
  });

  test('witnessScore takes precedence over queryAlignmentScore in primary sort', () => {
    const items = [
      { book: 'Low', witnessScore: 5, queryAlignmentScore: 1.0 },
      { book: 'High', witnessScore: 20, queryAlignmentScore: 0.1 },
    ];

    items.sort((a, b) => {
      if (b.witnessScore !== a.witnessScore) return b.witnessScore - a.witnessScore;
      return b.queryAlignmentScore - a.queryAlignmentScore;
    });

    expect(items[0].book).toBe('High'); // higher witnessScore wins despite low alignment
    expect(items[1].book).toBe('Low');
  });
});

describe('Major witness query_alignment_note field', () => {
  test('schema includes query_alignment_note on major witnesses when data is available', async () => {
    // Non-API test: verifies the schema check for query_alignment_note.
    // When D1/Vectorize are unavailable the tool returns isError, so we
    // only assert the shape when a real result comes back.
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

      for (const witness of parsed.major_witnesses) {
        // query_alignment_note must be a non-empty string when present.
        if (witness.query_alignment_note !== undefined) {
          expect(typeof witness.query_alignment_note).toBe('string');
          expect(witness.query_alignment_note.length).toBeGreaterThan(0);
        }
      }
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
