import { test, expect } from '@playwright/test';
import { isLiveAgent, sendMessage } from './helpers';

test.describe('Scenario 1: Full Conversation Flow — Invoice to Payment Reconciliation', () => {
  test('completes full agentic recipe building flow', async ({ page }) => {
    await page.goto('/reconcile');
    await expect(page.getByText('Recipe Builder')).toBeVisible();

    await page.getByRole('button', { name: 'Start Conversation' }).click();

    // Wait for first agent message (greeting or error)
    const firstAgent = page.locator('[data-testid="agent-message"]').first();
    await expect(firstAgent).toBeVisible({ timeout: 60_000 });

    const firstText = (await firstAgent.textContent()) ?? '';
    expect(firstText.length).toBeGreaterThan(10);

    const live = isLiveAgent(firstText);

    if (live) {
      // Greeting should mention data sources
      const lower = firstText.toLowerCase();
      expect(
        lower.includes('invoice') || lower.includes('payment') || lower.includes('source'),
      ).toBe(true);
    }

    // State intent
    const secondText = await sendMessage(page, 'I want to reconcile invoices with payments');

    if (live) {
      // Phase should advance beyond greeting
      const phaseIndicator = page.locator(
        'text=/intent|sampling|demonstration|inference|validation|execution/i',
      );
      await expect(phaseIndicator).toBeVisible({ timeout: 10_000 });
    }

    // Ask about sources
    const thirdText = await sendMessage(page, 'Show me what data sources are available');

    if (live) {
      const lower = thirdText.toLowerCase();
      expect(lower.includes('invoice') || lower.includes('payment')).toBe(true);
    }

    // Verify conversation has built up
    const agentMessages = page.locator('[data-testid="agent-message"]');
    const count = await agentMessages.count();
    expect(count).toBeGreaterThanOrEqual(3);

    // Verify phase indicator visible
    const phaseIndicator = page.locator(
      'text=/greeting|intent|sampling|demonstration|inference|validation|execution/i',
    );
    await expect(phaseIndicator).toBeVisible();
  });
});
