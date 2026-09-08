import { test, expect } from '@playwright/test';

test.describe('YARN Explorer SPA E2E & Smoke Suite', () => {
  test('loads without console errors and renders core components', async ({ page }) => {
    const consoleErrors: string[] = [];
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });
    page.on('pageerror', (err) => {
      consoleErrors.push(err.message);
    });

    await page.goto('http://localhost:5173');
    await expect(page).toHaveTitle(/YARN/i);

    // Header & Navigation
    await expect(page.locator('header')).toBeVisible();
    await expect(page.getByText('YARN Explorer')).toBeVisible();

    // Zero Console Errors Check
    expect(consoleErrors).toHaveLength(0);
  });

  test('interacts with login / user profile flow', async ({ page }) => {
    await page.goto('http://localhost:5173');
    const loginButton = page.getByRole('button', { name: /Войти в систему|Войти/i });
    if (await loginButton.isVisible()) {
      await loginButton.click();
      await expect(page.getByText(/Вход в YARN Explorer|Аутентификация/i)).toBeVisible();
    }
  });
});
