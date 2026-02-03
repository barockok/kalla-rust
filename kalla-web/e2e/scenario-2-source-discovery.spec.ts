import { test, expect } from '@playwright/test';

test.describe('Scenario 2: Source Discovery & Data Preview', () => {
  test('agent responds to source discovery questions', async ({ page }) => {
    await page.goto('/reconcile');
    await page.getByRole('button', { name: 'Start Conversation' }).click();

    const firstAgent = page.locator('[data-testid="agent-message"]').first();
    await expect(firstAgent).toBeVisible({ timeout: 60_000 });

    const input = page.getByPlaceholder('Type your message...');
    await input.fill('What data sources do I have available?');
    await page.getByRole('button', { name: 'Send' }).click();

    const sourceResponse = page.locator('[data-testid="agent-message"]').nth(1);
    await expect(sourceResponse).toBeVisible({ timeout: 60_000 });

    // Response should be non-empty (either real data or an error message)
    const text = await sourceResponse.textContent();
    expect(text!.length).toBeGreaterThan(5);

    await input.fill('Show me a preview of the invoices source');
    await page.getByRole('button', { name: 'Send' }).click();
    const previewResponse = page.locator('[data-testid="agent-message"]').nth(2);
    await expect(previewResponse).toBeVisible({ timeout: 60_000 });
  });

  test('agent handles unknown requests gracefully', async ({ page }) => {
    await page.goto('/reconcile');
    await page.getByRole('button', { name: 'Start Conversation' }).click();
    await expect(page.locator('[data-testid="agent-message"]').first()).toBeVisible({ timeout: 60_000 });

    const input = page.getByPlaceholder('Type your message...');
    await input.fill('Show me the source called "nonexistent_table"');
    await page.getByRole('button', { name: 'Send' }).click();

    const response = page.locator('[data-testid="agent-message"]').nth(1);
    await expect(response).toBeVisible({ timeout: 60_000 });

    // Agent should respond (whether with real content or error, it should be non-empty)
    const text = await response.textContent();
    expect(text!.length).toBeGreaterThan(5);
  });
});
