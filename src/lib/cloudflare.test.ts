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

// ─── d1.batch() tests ─────────────────────────────────────────────────────────

describe('d1.batch()', () => {
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

  test('empty array input returns [] immediately without making HTTP call', async () => {
    const mockFetch = vi.fn();
    vi.stubGlobal('fetch', mockFetch);

    const result = await d1.batch([]);

    expect(result).toEqual([]);
    expect(mockFetch).not.toHaveBeenCalled();
  });

  test('single statement: correct URL construction, correct body shape, returns result', async () => {
    const mockResult: Array<{ results: Record<string, unknown>[]; meta: Record<string, unknown>; success: boolean }> = [
      { results: [{ id: 1, name: 'Genesis' }], meta: { changes: 0, rows_read: 1 }, success: true },
    ];

    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ success: true, errors: [], result: mockResult }),
    });
    vi.stubGlobal('fetch', mockFetch);

    const result = await d1.batch([{ sql: 'SELECT * FROM books', params: [] }]);

    const expectedUrl = `${BASE}/accounts/test-account/d1/database/test-db/query`;
    expect(mockFetch).toHaveBeenCalledOnce();

    const [calledUrl, calledInit] = mockFetch.mock.calls[0];
    expect(calledUrl).toBe(expectedUrl);

    const body = JSON.parse(calledInit.body as string);
    expect(body).toEqual([{ sql: 'SELECT * FROM books', params: [] }]);

    expect(result).toHaveLength(1);
    expect(result[0].results).toEqual([{ id: 1, name: 'Genesis' }]);
  });

  test('multiple statements: body is array of {sql, params}, returns array of results in order', async () => {
    const mockResults = [
      { results: [{ count: 5 }], meta: { changes: 0, rows_read: 1 }, success: true },
      { results: [{ count: 10 }], meta: { changes: 0, rows_read: 1 }, success: true },
    ];

    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ success: true, errors: [], result: mockResults }),
    });
    vi.stubGlobal('fetch', mockFetch);

    const statements = [
      { sql: 'SELECT COUNT(*) AS count FROM verses', params: [] },
      { sql: 'SELECT COUNT(*) AS count FROM books WHERE testament = ?', params: ['OT'] },
    ];

    const result = await d1.batch(statements);

    const [, calledInit] = mockFetch.mock.calls[0];
    const body = JSON.parse(calledInit.body as string);
    expect(body).toEqual([
      { sql: 'SELECT COUNT(*) AS count FROM verses', params: [] },
      { sql: 'SELECT COUNT(*) AS count FROM books WHERE testament = ?', params: ['OT'] },
    ]);

    expect(result).toHaveLength(2);
    expect(result[0].results).toEqual([{ count: 5 }]);
    expect(result[1].results).toEqual([{ count: 10 }]);
  });

  test("throws descriptive error when result count doesn't match statement count", async () => {
    // API returns 1 result but we sent 2 statements
    const mockResults = [
      { results: [], meta: { changes: 0, rows_read: 0 }, success: true },
    ];

    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ success: true, errors: [], result: mockResults }),
    });
    vi.stubGlobal('fetch', mockFetch);

    await expect(
      d1.batch([
        { sql: 'SELECT 1' },
        { sql: 'SELECT 2' },
      ])
    ).rejects.toThrow(/D1 batch returned 1 results for 2 statements/);
  });

  test('throws on HTTP error (non-ok response)', async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: false,
      status: 403,
      statusText: 'Forbidden',
      text: async () => 'Authentication error',
    });
    vi.stubGlobal('fetch', mockFetch);

    await expect(
      d1.batch([{ sql: 'SELECT 1' }])
    ).rejects.toThrow(/Cloudflare API error: 403 Forbidden/);
  });
});
