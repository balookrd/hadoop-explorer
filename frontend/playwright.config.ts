import { defineConfig, devices } from '@playwright/test';

/**
 * Playwright E2E and Visual Regression Config
 * Hadoop Explorer Platform UI Test Suite
 */
export default defineConfig({
  testDir: './e2e',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: [['list'], ['html', { open: 'never' }]],
  use: {
    baseURL: 'http://localhost:5173',
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
    viewport: { width: 1280, height: 720 },
  },

  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],

  webServer: [
    {
      command: 'npm run dev:yarn -- --port 5173 --host',
      port: 5173,
      reuseExistingServer: !process.env.CI,
      timeout: 120000,
    },
    {
      command: 'npm run dev:hdfs -- --port 5174 --host',
      port: 5174,
      reuseExistingServer: !process.env.CI,
      timeout: 120000,
    },
    {
      command: 'npm run dev:sql -- --port 5175 --host',
      port: 5175,
      reuseExistingServer: !process.env.CI,
      timeout: 120000,
    },
    {
      command: 'npm run dev:spark -- --port 5176 --host',
      port: 5176,
      reuseExistingServer: !process.env.CI,
      timeout: 120000,
    },
  ],
});
