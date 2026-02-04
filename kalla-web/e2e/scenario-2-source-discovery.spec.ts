import { test, expect } from '@playwright/test';
import { isLiveAgent, sendMessage } from './helpers';

test.describe('Scenario 2: Source Discovery & Data Preview', () => {
  test('agent responds to source discovery questions', async ({ page }) => {
    await page.goto('/reconcile');
    await page.getByRole('button', { name: 'Start Conversation' }).click();

    const firstAgent = page.locator('[data-testid="agent-message"]').first();
    await expect(firstAgent).toBeVisible({ timeout: 60_000 });

    const firstText = (await firstAgent.textContent()) ?? '';
    const live = isLiveAgent(firstText);

    // Ask about data sources
    const sourceText = await sendMessage(page, 'What data sources do I have available?');
    expect(sourceText.length).toBeGreaterThan(5);

    if (live) {
      const lower = sourceText.toLowerCase();
      expect(lower.includes('invoice') || lower.includes('payment')).toBe(true);
    }

    // Ask for invoice preview
    const previewText = await sendMessage(page, 'Show me a preview of the invoices source');

    if (live) {
      const lower = previewText.toLowerCase();
      // Agent should discuss the invoices source — either show columns or explain access status
      expect(
        lower.includes('invoice_id') ||
          lower.includes('customer_name') ||
          lower.includes('amount') ||
          lower.includes('column') ||
          lower.includes('invoice') ||
          lower.includes('source') ||
          lower.includes('table'),
      ).toBe(true);
    }
  });

  test('agent handles unknown requests gracefully', async ({ page }) => {
    await page.goto('/reconcile');
    await page.getByRole('button', { name: 'Start Conversation' }).click();
    await expect(page.locator('[data-testid="agent-message"]').first()).toBeVisible({
      timeout: 60_000,
    });

    const firstText = (await page.locator('[data-testid="agent-message"]').first().textContent()) ?? '';
    const live = isLiveAgent(firstText);

    const responseText = await sendMessage(page, 'Show me the source called "nonexistent_table"');
    expect(responseText.length).toBeGreaterThan(5);

    if (live) {
      const lower = responseText.toLowerCase();
      // Should indicate not found or list available sources instead
      expect(
        lower.includes('not found') ||
          lower.includes('not available') ||
          lower.includes("doesn't exist") ||
          lower.includes('does not exist') ||
          lower.includes('available') ||
          lower.includes('invoice') ||
          lower.includes('payment'),
      ).toBe(true);
    }
  });
});
