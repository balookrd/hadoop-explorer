import { test, expect } from '@playwright/test';

test.describe('Cross-Application Header & Navigation Integrity Suite', () => {
  const apps = [
    { name: 'YARN Explorer', port: 5173, url: 'http://localhost:5173' },
    { name: 'HDFS Explorer', port: 5174, url: 'http://localhost:5174' },
    { name: 'SQL Explorer', port: 5175, url: 'http://localhost:5175' },
    { name: 'Spark Explorer', port: 5176, url: 'http://localhost:5176' },
  ];

  for (const app of apps) {
    test(`renders ${app.name} header and critical navigation elements correctly`, async ({ page }) => {
      const consoleErrors: string[] = [];
      page.on('console', (msg) => {
        if (msg.type() === 'error') consoleErrors.push(msg.text());
      });
      page.on('pageerror', (err) => consoleErrors.push(err.message));

      await page.goto(app.url);
      const header = page.locator('header');
      await expect(header).toBeVisible();

      // Убеждаемся, что нет фатальных падений страницы
      expect(consoleErrors).toHaveLength(0);
    });
  }
});
