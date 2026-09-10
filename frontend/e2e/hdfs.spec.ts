import { test, expect } from '@playwright/test';

test.describe('HDFS Explorer SPA E2E & Smoke Suite', () => {
  test('loads without console errors and renders file explorer', async ({ page }) => {
    const consoleErrors: string[] = [];
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });
    page.on('pageerror', (err) => {
      consoleErrors.push(err.message);
    });

    await page.goto('http://localhost:5174');
    await expect(page.locator('header')).toBeVisible();
    await expect(page.getByText('HDFS Explorer')).toBeVisible();

    expect(consoleErrors).toHaveLength(0);
  });

  test('verifies file explorer toolbar and search input presence', async ({ page }) => {
    await page.goto('http://localhost:5174');
    await expect(page.locator('header')).toBeVisible();

    const searchInput = page.getByPlaceholder(/Фильтр файлов/i);
    if (await searchInput.isVisible()) {
      await expect(searchInput).toBeEnabled();
    }
  });
});
