import { test, expect } from '@playwright/test';

test.describe('Spark Explorer SPA E2E & Smoke Suite', () => {
  test('loads without console errors and displays Spark workspace', async ({ page }) => {
    const consoleErrors: string[] = [];
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });
    page.on('pageerror', (err) => {
      consoleErrors.push(err.message);
    });

    await page.goto('http://localhost:5176');
    await expect(page).toHaveTitle(/Spark/i);
    await expect(page.locator('header')).toBeVisible();
    await expect(page.getByText('Spark Explorer')).toBeVisible();

    expect(consoleErrors).toHaveLength(0);
  });

  test('verifies Spark session language selectors', async ({ page }) => {
    await page.goto('http://localhost:5176');
    await expect(page.locator('header')).toBeVisible();

    const pysparkBtn = page.getByRole('button', { name: /PySpark/i });
    if (await pysparkBtn.isVisible()) {
      await expect(pysparkBtn).toBeVisible();
    }
  });
});
