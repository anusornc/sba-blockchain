import { chromium } from '@playwright/test';
import { mkdir } from 'node:fs/promises';
import { resolve } from 'node:path';

const baseUrl = process.env.BASE_URL || 'http://127.0.0.1:5174';
const output = resolve(
  process.env.SCREENSHOT_PATH || 'test-results/ontology-traceability-smoke.png',
);

const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });

await page.goto(baseUrl);
await page.getByRole('heading', { name: /Ontology Traceability Console/i }).waitFor();
await page.getByRole('heading', { name: 'Chocolate UHT Milk 1L' }).first().waitFor();
await page.locator('.trace-graph canvas').first().waitFor();

await mkdir(resolve(output, '..'), { recursive: true });
await page.screenshot({ path: output, fullPage: true });
await browser.close();

console.log(`Wrote ${output}`);
