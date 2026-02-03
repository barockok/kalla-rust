import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './e2e',
  fullyParallel: false,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: 1,
  reporter: 'html',
  timeout: 120_000,
  expect: {
    timeout: 30_000,
  },
  use: {
    baseURL: 'http://localhost:3002',
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
  webServer: [
    {
      command: 'echo "Rust backend expected on port 3001"',
      port: 3001,
      reuseExistingServer: true,
    },
    {
      command: 'npx next dev --port 3002',
      port: 3002,
      reuseExistingServer: false,
      env: {
        ANTHROPIC_API_KEY: process.env.ANTHROPIC_API_KEY || '',
        NEXT_PUBLIC_API_URL: 'http://localhost:3001',
      },
    },
  ],
});
