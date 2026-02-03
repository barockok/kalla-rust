import { test, expect } from '@playwright/test';

test.describe('Scenario 3: Session Management & Reset', () => {
  test('maintains conversation context across multiple messages', async ({ page }) => {
    await page.goto('/reconcile');
    await page.getByRole('button', { name: 'Start Conversation' }).click();
    await expect(page.locator('[class*="bg-muted"]').first()).toBeVisible({ timeout: 60_000 });

    const input = page.getByPlaceholder('Type your message...');

    await input.fill('I want to work with invoices and payments');
    await page.getByRole('button').filter({ has: page.locator('svg') }).last().click();
    await expect(page.locator('[class*="bg-muted"]').nth(1)).toBeVisible({ timeout: 60_000 });

    await input.fill('Tell me more about the invoices source — what columns does it have?');
    await page.getByRole('button').filter({ has: page.locator('svg') }).last().click();
    await expect(page.locator('[class*="bg-muted"]').nth(2)).toBeVisible({ timeout: 60_000 });

    const allMessages = page.locator('[class*="rounded-lg"][class*="px-4"][class*="py-2"]');
    const messageCount = await allMessages.count();
    expect(messageCount).toBeGreaterThanOrEqual(5);

    await input.fill('What about the customer_name column? Is it useful for matching?');
    await page.getByRole('button').filter({ has: page.locator('svg') }).last().click();
    await expect(page.locator('[class*="bg-muted"]').nth(3)).toBeVisible({ timeout: 60_000 });
    const contextResponse = page.locator('[class*="bg-muted"]').nth(3);
    await expect(contextResponse).toBeVisible();
  });

  test('reset clears conversation and starts fresh', async ({ page }) => {
    await page.goto('/reconcile');
    await page.getByRole('button', { name: 'Start Conversation' }).click();
    await expect(page.locator('[class*="bg-muted"]').first()).toBeVisible({ timeout: 60_000 });

    const input = page.getByPlaceholder('Type your message...');
    await input.fill('Show me invoices');
    await page.getByRole('button').filter({ has: page.locator('svg') }).last().click();
    await expect(page.locator('[class*="bg-muted"]').nth(1)).toBeVisible({ timeout: 60_000 });

    const messagesBeforeReset = page.locator('[class*="bg-muted"]');
    const countBefore = await messagesBeforeReset.count();
    expect(countBefore).toBeGreaterThanOrEqual(2);

    await page.getByRole('button', { name: 'Reset' }).click();
    await expect(page.getByRole('button', { name: 'Start Conversation' })).toBeVisible();
    await expect(page.locator('[class*="bg-muted"]')).toHaveCount(0);

    await page.getByRole('button', { name: 'Start Conversation' }).click();
    await expect(page.locator('[class*="bg-muted"]').first()).toBeVisible({ timeout: 60_000 });
    const messagesAfterReset = page.locator('[class*="bg-muted"]');
    const countAfter = await messagesAfterReset.count();
    expect(countAfter).toBe(1);
  });

  test('chat API returns proper session structure', async ({ request }) => {
    const response = await request.post('/api/chat', {
      data: { message: 'Hello, what sources do I have?' },
    });
    expect(response.ok()).toBeTruthy();
    const body = await response.json();

    expect(body).toHaveProperty('session_id');
    expect(body).toHaveProperty('phase');
    expect(body).toHaveProperty('status');
    expect(body).toHaveProperty('message');
    expect(body.message).toHaveProperty('role', 'agent');
    expect(body.message).toHaveProperty('segments');
    expect(body.message).toHaveProperty('timestamp');
    expect(body.message.segments.length).toBeGreaterThan(0);

    const followUp = await request.post('/api/chat', {
      data: { session_id: body.session_id, message: 'Tell me about the invoices source' },
    });
    expect(followUp.ok()).toBeTruthy();
    const followUpBody = await followUp.json();
    expect(followUpBody.session_id).toBe(body.session_id);
  });
});
