import { type Page, expect } from '@playwright/test';

const ERROR_KEYWORDS = [
  'encountered an issue',
  'API',
  'credit balance',
  'error occurred',
  'something went wrong',
  'unable to process',
];

/** Returns true if the agent text looks like a real (non-error) response. */
export function isLiveAgent(text: string): boolean {
  const lower = text.toLowerCase();
  return !ERROR_KEYWORDS.some((kw) => lower.includes(kw.toLowerCase()));
}

/**
 * Sends a message in the chat UI and waits for the next agent response.
 * Returns the text content of the new agent message.
 */
export async function sendMessage(page: Page, text: string): Promise<string> {
  const agentMessages = page.locator('[data-testid="agent-message"]');
  const countBefore = await agentMessages.count();

  const input = page.getByPlaceholder('Type your message...');
  await input.fill(text);
  await page.getByRole('button', { name: 'Send' }).click();

  const nextAgent = agentMessages.nth(countBefore);
  await expect(nextAgent).toBeVisible({ timeout: 60_000 });

  const content = await nextAgent.textContent();
  return content ?? '';
}
