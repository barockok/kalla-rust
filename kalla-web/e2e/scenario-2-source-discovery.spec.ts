import { test, expect } from '@playwright/test';

test.describe('Scenario 2: Source Discovery & Data Preview', () => {
  test('agent lists sources and shows data previews', async ({ page }) => {
    await page.goto('/reconcile');
    await page.getByRole('button', { name: 'Start Conversation' }).click();
    const firstMessage = page.locator('[class*="bg-muted"]').first();
    await expect(firstMessage).toBeVisible({ timeout: 60_000 });

    const input = page.getByPlaceholder('Type your message...');
    await input.fill('What data sources do I have available?');
    await page.getByRole('button').filter({ has: page.locator('svg') }).last().click();

    const sourceResponse = page.locator('[class*="bg-muted"]').nth(1);
    await expect(sourceResponse).toBeVisible({ timeout: 60_000 });
    await expect(sourceResponse).toContainText(/invoices|payments/i, { timeout: 10_000 });

    await input.fill('Show me a preview of the invoices source');
    await page.getByRole('button').filter({ has: page.locator('svg') }).last().click();
    const previewResponse = page.locator('[class*="bg-muted"]').nth(2);
    await expect(previewResponse).toBeVisible({ timeout: 60_000 });
    await expect(previewResponse).toContainText(/invoice_id|customer|amount/i, { timeout: 10_000 });

    await input.fill('Now show me the payments source');
    await page.getByRole('button').filter({ has: page.locator('svg') }).last().click();
    const paymentsPreview = page.locator('[class*="bg-muted"]').nth(3);
    await expect(paymentsPreview).toBeVisible({ timeout: 60_000 });
    await expect(paymentsPreview).toContainText(/payment_id|payer|amount|paid_amount/i, { timeout: 10_000 });
  });

  test('agent handles non-existent source gracefully', async ({ page }) => {
    await page.goto('/reconcile');
    await page.getByRole('button', { name: 'Start Conversation' }).click();
    await expect(page.locator('[class*="bg-muted"]').first()).toBeVisible({ timeout: 60_000 });

    const input = page.getByPlaceholder('Type your message...');
    await input.fill('Show me the source called "nonexistent_table"');
    await page.getByRole('button').filter({ has: page.locator('svg') }).last().click();

    const response = page.locator('[class*="bg-muted"]').nth(1);
    await expect(response).toBeVisible({ timeout: 60_000 });
    await expect(response).toContainText(/not found|doesn't exist|error|available/i, { timeout: 10_000 });
  });
});
