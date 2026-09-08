import { test, expect } from '@playwright/test';

test.describe('SQL Explorer SPA E2E & Smoke Suite', () => {
  test('loads without console errors and displays query editor interface', async ({ page }) => {
    const consoleErrors: string[] = [];
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });
    page.on('pageerror', (err) => {
      consoleErrors.push(err.message);
    });

    await page.goto('http://localhost:5175');
    await expect(page).toHaveTitle(/SQL/i);
    await expect(page.locator('header')).toBeVisible();

    expect(consoleErrors).toHaveLength(0);
  });
});
