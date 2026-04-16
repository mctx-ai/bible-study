import { describe, test, expect, vi, beforeEach, afterEach } from 'vitest';
import { getConfig, resetConfigForTesting, d1 } from './cloudflare.js';

const BASE = 'https://api.cloudflare.com/client/v4';

// ─── getConfig() tests ────────────────────────────────────────────────────────

describe('getConfig()', () => {
  beforeEach(() => {
    resetConfigForTesting();
  });

  afterEach(() => {
    vi.unstubAllEnvs();
  });

  test('returns cached config on subsequent calls (same reference)', () => {
    vi.stubEnv('BIBLE_API_TOKEN', 'token-abc');
    vi.stubEnv('BIBLE_ACCOUNT_ID', 'account-abc');

    const first = getConfig();
    const second = getConfig();

    expect(first).toBe(second);
  });

  test('reads BIBLE_API_TOKEN and BIBLE_ACCOUNT_ID as primary env vars', () => {
    vi.stubEnv('BIBLE_API_TOKEN', 'bible-token');
    vi.stubEnv('BIBLE_ACCOUNT_ID', 'bible-account');
    vi.stubEnv('CLOUDFLARE_API_TOKEN', 'cf-token');
    vi.stubEnv('CLOUDFLARE_ACCOUNT_ID', 'cf-account');

    const config = getConfig();

    expect(config.apiToken).toBe('bible-token');
    expect(config.accountId).toBe('bible-account');
  });

  test('falls back to CLOUDFLARE_API_TOKEN and CLOUDFLARE_ACCOUNT_ID when BIBLE_* not set', () => {
    vi.stubEnv('CLOUDFLARE_API_TOKEN', 'cf-token-fallback');
    vi.stubEnv('CLOUDFLARE_ACCOUNT_ID', 'cf-account-fallback');

    const config = getConfig();

    expect(config.apiToken).toBe('cf-token-fallback');
    expect(config.accountId).toBe('cf-account-fallback');
  });

  test('returns empty strings when no env vars set', () => {
    // Ensure these are not set
    vi.stubEnv('BIBLE_API_TOKEN', '');
    vi.stubEnv('BIBLE_ACCOUNT_ID', '');
    vi.stubEnv('CLOUDFLARE_API_TOKEN', '');
    vi.stubEnv('CLOUDFLARE_ACCOUNT_ID', '');
    vi.stubEnv('D1_DATABASE_ID', '');
    vi.stubEnv('VECTORIZE_INDEX_NAME', '');

    const config = getConfig();

    expect(config.apiToken).toBe('');
    expect(config.accountId).toBe('');
    expect(config.databaseId).toBe('');
    expect(config.indexName).toBe('');
  });

  test('reads D1_DATABASE_ID and VECTORIZE_INDEX_NAME directly (no fallback prefix)', () => {
    vi.stubEnv('D1_DATABASE_ID', 'my-db-id');
    vi.stubEnv('VECTORIZE_INDEX_NAME', 'my-index');

    const config = getConfig();

    expect(config.databaseId).toBe('my-db-id');
    expect(config.indexName).toBe('my-index');
  });
});

// ─── d1.query() tests ─────────────────────────────────────────────────────────

describe('d1.query()', () => {
  beforeEach(() => {
    resetConfigForTesting();
    vi.stubEnv('BIBLE_API_TOKEN', 'test-token');
    vi.stubEnv('BIBLE_ACCOUNT_ID', 'test-account');
    vi.stubEnv('D1_DATABASE_ID', 'test-db');
  });

  afterEach(() => {
    vi.unstubAllEnvs();
    vi.unstubAllGlobals();
  });

  test('correct URL construction, correct body shape, returns first result set', async () => {
    const mockResult = [
      { results: [{ id: 1, name: 'Genesis' }], meta: { changes: 0, rows_read: 1 }, success: true },
    ];

    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ success: true, errors: [], result: mockResult }),
    });
    vi.stubGlobal('fetch', mockFetch);

    const result = await d1.query('SELECT * FROM books', []);

    const expectedUrl = `${BASE}/accounts/test-account/d1/database/test-db/query`;
    expect(mockFetch).toHaveBeenCalledOnce();

    const [calledUrl, calledInit] = mockFetch.mock.calls[0];
    expect(calledUrl).toBe(expectedUrl);

    const body = JSON.parse(calledInit.body as string);
    expect(body).toEqual({ sql: 'SELECT * FROM books', params: [] });

    expect(result.results).toEqual([{ id: 1, name: 'Genesis' }]);
  });

  test('passes params correctly to the request body', async () => {
    const mockResult = [
      { results: [{ count: 5 }], meta: { changes: 0, rows_read: 1 }, success: true },
    ];

    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ success: true, errors: [], result: mockResult }),
    });
    vi.stubGlobal('fetch', mockFetch);

    await d1.query('SELECT COUNT(*) AS count FROM books WHERE testament = ?', ['OT']);

    const [, calledInit] = mockFetch.mock.calls[0];
    const body = JSON.parse(calledInit.body as string);
    expect(body).toEqual({
      sql: 'SELECT COUNT(*) AS count FROM books WHERE testament = ?',
      params: ['OT'],
    });
  });

  test('throws on empty result array from API', async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ success: true, errors: [], result: [] }),
    });
    vi.stubGlobal('fetch', mockFetch);

    await expect(d1.query('SELECT 1')).rejects.toThrow(
      /D1 query returned an unexpected empty result array/,
    );
  });

  test('throws on HTTP error (non-ok response)', async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: false,
      status: 403,
      statusText: 'Forbidden',
      text: async () => 'Authentication error',
    });
    vi.stubGlobal('fetch', mockFetch);

    await expect(d1.query('SELECT 1')).rejects.toThrow(/Cloudflare API error: 403 Forbidden/);
  });

  test('multiple concurrent queries via Promise.all return independent results', async () => {
    const mockResults1 = [
      { results: [{ id: 1 }], meta: { changes: 0, rows_read: 1 }, success: true },
    ];
    const mockResults2 = [
      { results: [{ id: 2 }], meta: { changes: 0, rows_read: 1 }, success: true },
    ];

    let callCount = 0;
    const mockFetch = vi.fn().mockImplementation(() => {
      callCount++;
      const result = callCount === 1 ? mockResults1 : mockResults2;
      return Promise.resolve({
        ok: true,
        json: async () => ({ success: true, errors: [], result }),
      });
    });
    vi.stubGlobal('fetch', mockFetch);

    const [r1, r2] = await Promise.all([d1.query('SELECT 1'), d1.query('SELECT 2')]);

    expect(mockFetch).toHaveBeenCalledTimes(2);
    expect(r1.results).toEqual([{ id: 1 }]);
    expect(r2.results).toEqual([{ id: 2 }]);
  });
});
