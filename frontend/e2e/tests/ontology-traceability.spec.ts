import { expect, test } from '@playwright/test';

test.describe('Ontology Traceability Frontend', () => {
  test('loads a QR trace and renders graph inspection UI', async ({ page }) => {
    await page.goto('/');

    await expect(page.getByRole('heading', { name: /Ontology Traceability Console/i })).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Chocolate UHT Milk 1L' }).first()).toBeVisible();
    await expect(page.getByText('deterministic-uht-dataset')).toBeVisible();
    await expect(page.getByText('10 nodes')).toBeVisible();
    await expect(page.getByText('9 edges')).toBeVisible();

    const graph = page.locator('.trace-graph');
    await expect(graph).toBeVisible();
    await expect(graph.locator('canvas').first()).toBeVisible();

    await expect(page.getByText('SBA / PROV-O View')).toBeVisible();
    await expect(page.getByText('Product Batch')).toBeVisible();
    await expect(page.getByText('prov:Activity')).toBeVisible();

    await page.getByRole('button', { name: /Plain UHT Batch/i }).click();
    await expect(page.getByRole('heading', { name: 'Plain Whole UHT Milk 1L' }).first()).toBeVisible();
    await expect(page.locator('small').filter({ hasText: 'UHT-PLAIN-CM-2024-001' })).toBeVisible();
  });
});
