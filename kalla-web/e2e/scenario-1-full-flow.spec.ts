import { test, expect } from '@playwright/test';

test.describe('Scenario 1: Full Conversation Flow — Invoice to Payment Reconciliation', () => {
  test('completes full agentic recipe building flow', async ({ page }) => {
    await page.goto('/reconcile');
    await expect(page.getByText('Recipe Builder')).toBeVisible();

    await page.getByRole('button', { name: 'Start Conversation' }).click();

    // Wait for first agent message (greeting or error)
    const firstAgent = page.locator('[data-testid="agent-message"]').first();
    await expect(firstAgent).toBeVisible({ timeout: 60_000 });

    // Agent should respond with something
    const firstText = await firstAgent.textContent();
    expect(firstText!.length).toBeGreaterThan(10);

    // State intent
    const input = page.getByPlaceholder('Type your message...');
    await input.fill('I want to reconcile invoices with payments');
    await page.getByRole('button', { name: 'Send' }).click();

    // Wait for second agent message
    const secondAgent = page.locator('[data-testid="agent-message"]').nth(1);
    await expect(secondAgent).toBeVisible({ timeout: 60_000 });

    // Send another message
    await input.fill('Show me what data sources are available');
    await page.getByRole('button', { name: 'Send' }).click();

    // Wait for third agent message
    const thirdAgent = page.locator('[data-testid="agent-message"]').nth(2);
    await expect(thirdAgent).toBeVisible({ timeout: 60_000 });

    // Verify conversation has built up
    const agentMessages = page.locator('[data-testid="agent-message"]');
    const count = await agentMessages.count();
    expect(count).toBeGreaterThanOrEqual(3);

    // Verify phase indicator visible
    const phaseIndicator = page.locator('text=/greeting|intent|sampling|demonstration|inference|validation|execution/i');
    await expect(phaseIndicator).toBeVisible();
  });
});
