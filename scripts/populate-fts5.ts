#!/usr/bin/env tsx
/**
 * populate-fts5.ts
 *
 * Populates (or rebuilds) the FTS5 full-text search index for the Bible Study MCP Server.
 *
 * The verses_fts virtual table is a content table backed by the verses table.
 * On first run this script inserts all verse text into the index. On re-runs it
 * issues the special rebuild command so the index stays consistent with the verses
 * table without leaving duplicate entries.
 *
 * Usage:
 *   npx tsx scripts/populate-fts5.ts
 *   npm run search:fts5
 *
 * Required environment variables:
 *   CLOUDFLARE_API_TOKEN
 *   CLOUDFLARE_ACCOUNT_ID
 *   D1_DATABASE_ID
 */

import './load-env.js';
import { d1 } from '../src/lib/cloudflare.js';

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

function log(msg: string): void {
  console.log(`[fts5] ${msg}`);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log('Bible Study MCP Server — FTS5 Index Population');
  console.log('=========================================\n');

  const required = ['CLOUDFLARE_API_TOKEN', 'CLOUDFLARE_ACCOUNT_ID', 'D1_DATABASE_ID'];
  const missing = required.filter((k) => !process.env[k]);
  if (missing.length > 0) {
    console.error(`ERROR: Missing required environment variables: ${missing.join(', ')}`);
    process.exit(1);
  }

  // Check whether the index already has content by counting rows in the shadow
  // content table. The FTS5 content= table stores no rows of its own; querying
  // verses_fts directly is the right probe.
  log('Checking current FTS5 index state...');
  const countResult = await d1.query('SELECT count(*) AS n FROM verses_fts');
  const existingCount = (countResult.results[0] as { n: number }).n;
  log(`  Found ${existingCount.toLocaleString()} rows in verses_fts`);

  if (existingCount > 0) {
    // Rebuild keeps the index consistent with the backing verses table, replacing
    // any stale or duplicate entries from previous partial runs.
    log('Index is non-empty — issuing rebuild...');
    await d1.query("INSERT INTO verses_fts(verses_fts) VALUES('rebuild')");
    log('  Rebuild complete');
  } else {
    // Populate from scratch: copy rowid + text for every verse.
    log('Index is empty — populating from verses table...');
    await d1.query('INSERT INTO verses_fts(rowid, text) SELECT id, text FROM verses');
    log('  Initial population complete');
  }

  // Verify with a known phrase that appears in every translation.
  log('Verifying with test query ("in the beginning")...');
  const verifyResult = await d1.query(
    `SELECT count(*) AS n FROM verses_fts WHERE verses_fts MATCH '"in the beginning"'`
  );
  const matchCount = (verifyResult.results[0] as { n: number }).n;
  log(`  Matched ${matchCount.toLocaleString()} verse(s) for "in the beginning"`);

  if (matchCount === 0) {
    console.error('ERROR: Verification query returned 0 matches — FTS5 index may be empty or broken');
    process.exit(1);
  }

  // Final row count for confirmation.
  const finalResult = await d1.query('SELECT count(*) AS n FROM verses_fts');
  const finalCount = (finalResult.results[0] as { n: number }).n;
  log(`  Total indexed rows: ${finalCount.toLocaleString()}`);

  console.log('\nDone.');
}

main().catch((err) => {
  console.error('[fts5] Unexpected error:', err);
  process.exit(1);
});
