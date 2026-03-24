// Resource: bible://translations
//
// Returns the list of all available Bible translations with abbreviation,
// full name, and year. Data is served from the in-memory cache populated at
// module load time — no D1 round-trip per request.

import type { ResourceHandler } from '@mctx-ai/app';
import { getAllTranslations, ensureInitialized } from '../lib/bible-utils.js';

const handler: ResourceHandler = async (_params) => {
  await ensureInitialized();
  const translations = getAllTranslations();
  return JSON.stringify(translations);
};

handler.description =
  'Lists all available Bible translations with their abbreviation, full name, and publication year.';
handler.mimeType = 'application/json';

export default handler;
