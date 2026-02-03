import { test, expect } from '@playwright/test';

test.describe('Scenario 1: Full Conversation Flow — Invoice to Payment Reconciliation', () => {
  test('completes full agentic recipe building flow', async ({ page }) => {
    await page.goto('/reconcile');
    await expect(page.getByText('Recipe Builder')).toBeVisible();

    await page.getByRole('button', { name: 'Start Conversation' }).click();

    // Wait for agent greeting
    await expect(page.locator('[class*="bg-muted"]').first()).toBeVisible({ timeout: 60_000 });
    const firstAgentMessage = page.locator('[class*="bg-muted"]').first();
    await expect(firstAgentMessage).toContainText(/source|data|invoices|payments/i, { timeout: 60_000 });

    // State intent
    const input = page.getByPlaceholder('Type your message...');
    await input.fill('I want to reconcile invoices with payments');
    await page.getByRole('button').filter({ has: page.locator('svg') }).last().click();
    await expect(page.locator('[class*="bg-muted"]').nth(1)).toBeVisible({ timeout: 60_000 });

    // Request samples
    await input.fill('Load a sample of the invoices and payments — all records are fine for now');
    await page.getByRole('button').filter({ has: page.locator('svg') }).last().click();
    await expect(page.locator('[class*="bg-muted"]').nth(2)).toBeVisible({ timeout: 60_000 });

    // Confirm a match
    await input.fill('INV-2024-001 matches PAY-2024-001 — same customer Acme Corporation, same amount $15,000');
    await page.getByRole('button').filter({ has: page.locator('svg') }).last().click();
    await expect(page.locator('[class*="bg-muted"]').nth(3)).toBeVisible({ timeout: 60_000 });

    // Provide another example
    await input.fill('INV-2024-002 also matches PAY-2024-002, both are $7,500.50 from TechStart Inc');
    await page.getByRole('button').filter({ has: page.locator('svg') }).last().click();
    await expect(page.locator('[class*="bg-muted"]').nth(4)).toBeVisible({ timeout: 60_000 });

    // Ask for recipe
    await input.fill('I think those examples are enough. Can you build a recipe from these patterns?');
    await page.getByRole('button').filter({ has: page.locator('svg') }).last().click();
    await expect(page.locator('[class*="bg-muted"]').nth(5)).toBeVisible({ timeout: 60_000 });

    // Verify conversation progressed
    const agentMessages = page.locator('[class*="bg-muted"]');
    const count = await agentMessages.count();
    expect(count).toBeGreaterThanOrEqual(6);

    // Verify phase indicator visible
    const phaseIndicator = page.locator('text=/greeting|intent|sampling|demonstration|inference|validation|execution/i');
    await expect(phaseIndicator).toBeVisible();
  });
});
