import React from 'react';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

// Mock scrollIntoView which is not available in jsdom
Element.prototype.scrollIntoView = jest.fn();

// Mock next/link
jest.mock('next/link', () => {
  return function Link({ href, children, ...props }: { href: string; children: React.ReactNode; [key: string]: unknown }) {
    return <a href={href} {...props}>{children}</a>;
  };
});

// Mock next/navigation
jest.mock('next/navigation', () => ({
  usePathname: () => '/',
  useParams: () => ({ id: 'test' }),
}));

// Mock @tanstack/react-query
jest.mock('@tanstack/react-query', () => ({
  useQuery: jest.fn(),
  QueryClient: jest.fn().mockImplementation(() => ({})),
  QueryClientProvider: ({ children }: { children: React.ReactNode }) => children,
}));

// Mock @/lib/api
jest.mock('@/lib/api', () => ({
  listSources: jest.fn(),
  registerSource: jest.fn(),
  validateRecipe: jest.fn(),
  listRuns: jest.fn(),
  getRun: jest.fn(),
}));

// Mock ChatMessage to expose onCardAction
jest.mock('@/components/chat/ChatMessage', () => ({
  ChatMessage: ({ message, onCardAction }: { message: { role: string; segments: Array<{ content?: string }> }; onCardAction?: (cardId: string, action: string, value?: unknown) => void }) => (
    <div data-testid={message.role === 'agent' ? 'agent-message' : 'user-message'}>
      {message.segments.map((s: { content?: string }, i: number) => (
        <span key={i}>{s.content}</span>
      ))}
      {onCardAction && (
        <button data-testid="card-action-btn" onClick={() => onCardAction('card-1', 'confirm', {})}>
          Card Action
        </button>
      )}
    </div>
  ),
}));

// Mock RecipeCard to display recipe data
jest.mock('@/components/chat/RecipeCard', () => ({
  RecipeCard: ({ recipe }: { recipe: unknown }) => (
    <div data-testid="recipe-card">{recipe ? JSON.stringify(recipe) : 'No recipe'}</div>
  ),
}));

// Mock fetch
const mockFetch = jest.fn();
global.fetch = mockFetch;

// Import page after mocks are set up
import ReconcilePage from '@/app/reconcile/page';

beforeEach(() => {
  jest.clearAllMocks();
  mockFetch.mockReset();
});

// Helper: returns a standard successful chat response
function makeChatResponse(overrides: Record<string, unknown> = {}) {
  return {
    ok: true,
    json: async () => ({
      session_id: 'sess-1',
      phase: 'greeting',
      message: {
        role: 'agent',
        segments: [{ type: 'text', content: 'Hello! What data would you like to reconcile?' }],
        timestamp: new Date().toISOString(),
      },
      ...overrides,
    }),
  };
}

// Helper: start the conversation and wait for the first response
async function startConversation() {
  await act(async () => {
    fireEvent.click(screen.getByText('Start Conversation'));
  });
  await waitFor(() => {
    expect(screen.getByTestId('agent-message')).toBeInTheDocument();
  });
}

// ---------------------------------------------------------------------------
// Test 1: Form submission via Enter key (covers handleSubmit lines 95-98)
// ---------------------------------------------------------------------------
describe('ReconcilePage - handleSubmit (form submission)', () => {
  it('submits user text via the form and shows it as a user message', async () => {
    const user = userEvent.setup();

    // First fetch: handleStart's sendMessage
    mockFetch.mockResolvedValueOnce(makeChatResponse());

    // Second fetch: handleSubmit's sendMessage
    mockFetch.mockResolvedValueOnce(makeChatResponse({
      message: {
        role: 'agent',
        segments: [{ type: 'text', content: 'Got it, processing your request.' }],
        timestamp: new Date().toISOString(),
      },
    }));

    render(<ReconcilePage />);
    await startConversation();

    // Type text into the input
    const input = screen.getByPlaceholderText('Type your message...');
    await user.type(input, 'I want to reconcile invoices{enter}');

    // Wait for the user message to appear
    await waitFor(() => {
      const userMessages = screen.getAllByTestId('user-message');
      const hasInvoiceMsg = userMessages.some(el => el.textContent?.includes('I want to reconcile invoices'));
      expect(hasInvoiceMsg).toBe(true);
    });

    // Verify the second fetch was called with the typed text
    await waitFor(() => {
      expect(mockFetch).toHaveBeenCalledTimes(2);
      const secondCallBody = JSON.parse(mockFetch.mock.calls[1][1].body);
      expect(secondCallBody.message).toBe('I want to reconcile invoices');
      expect(secondCallBody.card_response).toBeUndefined();
    });
  });
});

// ---------------------------------------------------------------------------
// Test 2: handleCardAction (covers line 77-78)
// ---------------------------------------------------------------------------
describe('ReconcilePage - handleCardAction', () => {
  it('clicking a card action button sends card_response without message', async () => {
    // First fetch: handleStart's sendMessage
    mockFetch.mockResolvedValueOnce(makeChatResponse());

    // Second fetch: handleCardAction's sendMessage
    mockFetch.mockResolvedValueOnce(makeChatResponse({
      message: {
        role: 'agent',
        segments: [{ type: 'text', content: 'Card action received.' }],
        timestamp: new Date().toISOString(),
      },
    }));

    render(<ReconcilePage />);
    await startConversation();

    // Both user and agent messages get a card action button from the mock.
    // Pick the one inside the agent message.
    const agentMessage = screen.getByTestId('agent-message');
    const cardActionBtn = agentMessage.querySelector('[data-testid="card-action-btn"]') as HTMLElement;
    expect(cardActionBtn).toBeInTheDocument();

    await act(async () => {
      fireEvent.click(cardActionBtn);
    });

    // Verify fetch was called with card_response and no message
    await waitFor(() => {
      expect(mockFetch).toHaveBeenCalledTimes(2);
      const secondCallBody = JSON.parse(mockFetch.mock.calls[1][1].body);
      expect(secondCallBody.card_response).toEqual({
        card_id: 'card-1',
        action: 'confirm',
        value: {},
      });
      expect(secondCallBody.message).toBeUndefined();
    });
  });
});

// ---------------------------------------------------------------------------
// Test 3: Recipe draft display (covers line 163 with recipe_draft)
// ---------------------------------------------------------------------------
describe('ReconcilePage - recipe draft display', () => {
  it('displays the recipe draft in RecipeCard when API returns recipe_draft', async () => {
    const recipeDraft = { version: '1.0', recipe_id: 'test' };

    // handleStart's sendMessage returns a recipe_draft
    mockFetch.mockResolvedValueOnce(makeChatResponse({
      recipe_draft: recipeDraft,
    }));

    render(<ReconcilePage />);
    await startConversation();

    // The RecipeCard mock renders JSON.stringify(recipe) when recipe is truthy
    await waitFor(() => {
      const recipeCard = screen.getByTestId('recipe-card');
      expect(recipeCard.textContent).toContain('"version":"1.0"');
      expect(recipeCard.textContent).toContain('"recipe_id":"test"');
    });
  });
});
