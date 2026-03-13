import { describe, test, expect, vi, beforeEach } from 'vitest';

// Mock the cloudflare module before importing bible-utils so that d1.query
// is under test control throughout all suites.
vi.mock('./cloudflare.js', () => ({
  d1: {
    query: vi.fn(),
  },
}));

// Import after mock registration.
import { d1 } from './cloudflare.js';
import {
  ensureInitialized,
  resetInitForTesting,
  getTranslation,
  getAllTranslations,
  resolveBook,
} from './bible-utils.js';

// ─── Fixture data ─────────────────────────────────────────────────────────────

const translationRows = [
  { id: 1, abbreviation: 'KJV', name: 'King James Version', year: 1769 },
  { id: 2, abbreviation: 'WEB', name: 'World English Bible', year: 2000 },
  { id: 3, abbreviation: 'DBY', name: 'Darby Bible', year: 1890 },
];

const bookRows = [
  {
    id: 1,
    abbreviation: 'Gen',
    name: 'Genesis',
    testament: 'OT',
    canonical_order: 1,
  },
  {
    id: 2,
    abbreviation: 'Exod',
    name: 'Exodus',
    testament: 'OT',
    canonical_order: 2,
  },
];

const aliasRows = [
  { alias: 'Gn', book_id: 1 },
  { alias: 'Ex', book_id: 2 },
];

// Helper: configures d1.query to return translation rows, book rows, then alias rows
// in sequence (matching the call order in loadTranslations then loadBooks).
function mockD1Success() {
  const mock = vi.mocked(d1.query);
  mock
    .mockResolvedValueOnce({
      results: translationRows,
      meta: { changes: 0, rows_read: 2 },
      success: true,
    })
    .mockResolvedValueOnce({
      results: bookRows,
      meta: { changes: 0, rows_read: 2 },
      success: true,
    })
    .mockResolvedValueOnce({
      results: aliasRows,
      meta: { changes: 0, rows_read: 2 },
      success: true,
    });
}

// ─── Test setup ───────────────────────────────────────────────────────────────

beforeEach(() => {
  // Reset module-level state so each test starts from a clean slate.
  resetInitForTesting();
  vi.mocked(d1.query).mockReset();

  // Set env vars so init() proceeds past the early-return guard.
  process.env.CLOUDFLARE_API_TOKEN = 'test-token';
  process.env.CLOUDFLARE_ACCOUNT_ID = 'test-account';
  process.env.D1_DATABASE_ID = 'test-db';
});

// ─── ensureInitialized() tests ────────────────────────────────────────────────

describe('ensureInitialized()', () => {
  test('first call triggers init() and populates caches', async () => {
    mockD1Success();

    await ensureInitialized();

    // Translations cache populated.
    const kjv = getTranslation('KJV');
    expect(kjv).toBeDefined();
    expect(kjv?.name).toBe('King James Version');
    expect(getAllTranslations()).toHaveLength(3);

    // Books cache populated (by name, abbreviation, and alias).
    expect(resolveBook('Genesis')).toBeDefined();
    expect(resolveBook('Gen')).toBeDefined();
    expect(resolveBook('Gn')).toBeDefined(); // alias
    expect(resolveBook('Exodus')).toBeDefined();

    // d1.query called exactly 3 times: translations, books, aliases.
    expect(d1.query).toHaveBeenCalledTimes(3);
  });

  test('second call is a no-op (does not call init() again)', async () => {
    mockD1Success();

    await ensureInitialized();
    await ensureInitialized();

    // Query called only during the first initialization.
    expect(d1.query).toHaveBeenCalledTimes(3);
  });

  test('getTranslation resolves "Darby" as a case-insensitive alias for DBY', async () => {
    mockD1Success();

    await ensureInitialized();

    // Both DBY and Darby should resolve to the same translation
    const dby = getTranslation('DBY');
    expect(dby).toBeDefined();
    expect(dby?.abbreviation).toBe('DBY');
    expect(dby?.name).toBe('Darby Bible');

    // Case-insensitive alias: "Darby" should resolve to DBY
    const darby = getTranslation('Darby');
    expect(darby).toBeDefined();
    expect(darby?.abbreviation).toBe('DBY');
    expect(darby?.name).toBe('Darby Bible');

    // Verify they're the same object reference
    expect(dby).toBe(darby);

    // Test case insensitivity
    expect(getTranslation('darby')).toBeDefined();
    expect(getTranslation('DARBY')).toBeDefined();
    expect(getTranslation('DaRbY')).toBeDefined();
  });

  test('concurrent calls share the same promise (init() runs only once)', async () => {
    mockD1Success();

    // Fire two calls simultaneously without awaiting either first.
    await Promise.all([ensureInitialized(), ensureInitialized()]);

    // Despite two concurrent calls, d1.query should have run only once.
    expect(d1.query).toHaveBeenCalledTimes(3);
  });

  test('on failure clears initPromise so next call retries', async () => {
    // First call: translations query rejects immediately.
    // loadTranslations and loadBooks run concurrently via Promise.all, so
    // loadBooks may still issue its 2 queries (books + aliases) even though
    // translations failed. To keep the test deterministic we pre-load all
    // expected calls for both the failing first attempt and the successful retry.
    const mock = vi.mocked(d1.query);

    // First attempt: translations fails; books and aliases queries succeed
    // (they run in parallel and are not cancelled when translations rejects).
    mock
      .mockRejectedValueOnce(new Error('network error')) // translations (fail)
      .mockResolvedValueOnce({
        results: bookRows,
        meta: { changes: 0, rows_read: 2 },
        success: true,
      }) // books (first attempt)
      .mockResolvedValueOnce({
        results: aliasRows,
        meta: { changes: 0, rows_read: 2 },
        success: true,
      }); // aliases (first attempt)

    await ensureInitialized(); // should swallow error internally

    // Cache must still be empty after failure (initialized stays false).
    expect(getAllTranslations()).toHaveLength(0); // translations query failed, so no translations cached

    // Configure the successful retry — all 3 queries.
    mockD1Success();

    await ensureInitialized(); // should trigger init() again

    // After retry, cache is populated.
    expect(getAllTranslations()).toHaveLength(3);
    // Total: 3 (first failed attempt) + 3 (successful retry) = 6.
    expect(d1.query).toHaveBeenCalledTimes(6);
  });

  test('initialized flag is true after successful init', async () => {
    mockD1Success();

    // Verify pre-condition: caches empty before init.
    expect(getAllTranslations()).toHaveLength(0);

    await ensureInitialized();

    // A subsequent call must not hit d1.query again, confirming the flag is set.
    await ensureInitialized();
    expect(d1.query).toHaveBeenCalledTimes(3);
    expect(getAllTranslations()).toHaveLength(3);
  });
});
