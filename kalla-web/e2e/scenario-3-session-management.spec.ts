import { test, expect } from '@playwright/test';

test.describe('Scenario 3: Session Management & Reset', () => {
  test('maintains conversation context across multiple messages', async ({ page }) => {
    await page.goto('/reconcile');
    await page.getByRole('button', { name: 'Start Conversation' }).click();
    await expect(page.locator('[data-testid="agent-message"]').first()).toBeVisible({ timeout: 60_000 });

    const input = page.getByPlaceholder('Type your message...');

    await input.fill('I want to work with invoices and payments');
    await page.getByRole('button', { name: 'Send' }).click();
    await expect(page.locator('[data-testid="agent-message"]').nth(1)).toBeVisible({ timeout: 60_000 });

    await input.fill('Tell me more about the invoices source');
    await page.getByRole('button', { name: 'Send' }).click();
    await expect(page.locator('[data-testid="agent-message"]').nth(2)).toBeVisible({ timeout: 60_000 });

    // Verify messages accumulated (user + agent messages)
    const userMessages = page.locator('[data-testid="user-message"]');
    const agentMessages = page.locator('[data-testid="agent-message"]');
    const userCount = await userMessages.count();
    const agentCount = await agentMessages.count();
    expect(userCount).toBeGreaterThanOrEqual(2);
    expect(agentCount).toBeGreaterThanOrEqual(3);
  });

  test('reset clears conversation and starts fresh', async ({ page }) => {
    await page.goto('/reconcile');
    await page.getByRole('button', { name: 'Start Conversation' }).click();
    await expect(page.locator('[data-testid="agent-message"]').first()).toBeVisible({ timeout: 60_000 });

    const input = page.getByPlaceholder('Type your message...');
    await input.fill('Show me invoices');
    await page.getByRole('button', { name: 'Send' }).click();
    await expect(page.locator('[data-testid="agent-message"]').nth(1)).toBeVisible({ timeout: 60_000 });

    const agentsBefore = page.locator('[data-testid="agent-message"]');
    const countBefore = await agentsBefore.count();
    expect(countBefore).toBeGreaterThanOrEqual(2);

    await page.getByRole('button', { name: 'Reset' }).click();
    await expect(page.getByRole('button', { name: 'Start Conversation' })).toBeVisible();
    await expect(page.locator('[data-testid="agent-message"]')).toHaveCount(0);

    await page.getByRole('button', { name: 'Start Conversation' }).click();
    await expect(page.locator('[data-testid="agent-message"]').first()).toBeVisible({ timeout: 60_000 });
    const agentsAfter = page.locator('[data-testid="agent-message"]');
    const countAfter = await agentsAfter.count();
    expect(countAfter).toBe(1);
  });

  test('chat API returns proper session structure', async ({ request }) => {
    const response = await request.post('/api/chat', {
      data: { message: 'Hello, what sources do I have?' },
    });
    // Should return 200 even on API errors (graceful degradation)
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
