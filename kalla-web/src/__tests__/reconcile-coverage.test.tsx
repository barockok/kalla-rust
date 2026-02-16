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

// Mock upload-client
jest.mock('@/lib/upload-client', () => ({
  uploadFile: jest.fn(),
}));

// Mock ChatMessage to expose onCardAction and onFileUploaded
jest.mock('@/components/chat/ChatMessage', () => ({
  ChatMessage: ({ message, onCardAction, onFileUploaded }: { message: { role: string; segments: Array<{ content?: string }> }; onCardAction?: (cardId: string, action: string, value?: unknown) => void; onFileUploaded?: (a: unknown) => void }) => (
    <div data-testid={message.role === 'agent' ? 'agent-message' : 'user-message'}>
      {message.segments.map((s: { content?: string }, i: number) => (
        <span key={i}>{s.content}</span>
      ))}
      {onCardAction && (
        <button data-testid="card-action-btn" onClick={() => onCardAction('card-1', 'confirm', {})}>
          Card Action
        </button>
      )}
      {onFileUploaded && (
        <button data-testid="file-uploaded-btn" onClick={() => onFileUploaded({ upload_id: 'u1', filename: 'test.csv', s3_uri: 's3://b/k', columns: ['a'], row_count: 1 })}>
          File Uploaded
        </button>
      )}
    </div>
  ),
}));

// Mock FileUploadPill
jest.mock('@/components/chat/FileUploadPill', () => ({
  FileUploadPill: ({ filename, onRemove }: { filename: string; onRemove: () => void }) => (
    <div data-testid="file-pill">
      <span>{filename}</span>
      <button data-testid="remove-file" onClick={onRemove}>Remove</button>
    </div>
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
// Test 3: Error handling (covers sendMessage catch block, lines 82-83,91-95)
// ---------------------------------------------------------------------------
describe('ReconcilePage - error handling', () => {
  it('shows error message when fetch fails', async () => {
    // handleStart succeeds
    mockFetch.mockResolvedValueOnce(makeChatResponse());

    render(<ReconcilePage />);
    await startConversation();

    // Next fetch fails
    mockFetch.mockResolvedValueOnce({
      ok: false,
      statusText: 'Internal Server Error',
      json: async () => ({ error: 'Something broke' }),
    });

    const user = userEvent.setup();
    const input = screen.getByPlaceholderText('Type your message...');
    await user.type(input, 'test error{enter}');

    await waitFor(() => {
      const agentMessages = screen.getAllByTestId('agent-message');
      const hasError = agentMessages.some(el => el.textContent?.includes('Error: Something broke'));
      expect(hasError).toBe(true);
    });
  });

  it('shows fallback error when fetch throws', async () => {
    mockFetch.mockResolvedValueOnce(makeChatResponse());

    render(<ReconcilePage />);
    await startConversation();

    mockFetch.mockRejectedValueOnce(new Error('Network down'));

    const user = userEvent.setup();
    const input = screen.getByPlaceholderText('Type your message...');
    await user.type(input, 'trigger error{enter}');

    await waitFor(() => {
      const agentMessages = screen.getAllByTestId('agent-message');
      const hasError = agentMessages.some(el => el.textContent?.includes('Error: Network down'));
      expect(hasError).toBe(true);
    });
  });

  it('shows fallback error when json() also fails', async () => {
    mockFetch.mockResolvedValueOnce(makeChatResponse());

    render(<ReconcilePage />);
    await startConversation();

    mockFetch.mockResolvedValueOnce({
      ok: false,
      statusText: 'Bad Gateway',
      json: async () => { throw new Error('not json'); },
    });

    const user = userEvent.setup();
    const input = screen.getByPlaceholderText('Type your message...');
    await user.type(input, 'trigger{enter}');

    await waitFor(() => {
      const agentMessages = screen.getAllByTestId('agent-message');
      const hasError = agentMessages.some(el => el.textContent?.includes('Error: Bad Gateway'));
      expect(hasError).toBe(true);
    });
  });
});

// ---------------------------------------------------------------------------
// Test 4: Reset button (covers handleReset, lines 100-101)
// ---------------------------------------------------------------------------
describe('ReconcilePage - reset', () => {
  it('clicking Reset returns to start screen', async () => {
    mockFetch.mockResolvedValueOnce(makeChatResponse());

    render(<ReconcilePage />);
    await startConversation();

    // Verify conversation is active
    expect(screen.getByText('Reset')).toBeInTheDocument();

    await act(async () => {
      fireEvent.click(screen.getByText('Reset'));
    });

    // Should show start screen again
    expect(screen.getByText('Start Conversation')).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// Test 5: File uploaded via agent card (covers handleFileUploaded, line 164)
// ---------------------------------------------------------------------------
describe('ReconcilePage - handleFileUploaded', () => {
  it('sends message with file attachment when agent card triggers upload', async () => {
    mockFetch.mockResolvedValueOnce(makeChatResponse());
    // Response for the file upload message
    mockFetch.mockResolvedValueOnce(makeChatResponse({
      message: {
        role: 'agent',
        segments: [{ type: 'text', content: 'File received!' }],
        timestamp: new Date().toISOString(),
      },
    }));

    render(<ReconcilePage />);
    await startConversation();

    const agentMessage = screen.getByTestId('agent-message');
    const fileBtn = agentMessage.querySelector('[data-testid="file-uploaded-btn"]') as HTMLElement;
    expect(fileBtn).toBeInTheDocument();

    await act(async () => {
      fireEvent.click(fileBtn);
    });

    await waitFor(() => {
      expect(mockFetch).toHaveBeenCalledTimes(2);
      const body = JSON.parse(mockFetch.mock.calls[1][1].body);
      expect(body.files).toEqual([{
        upload_id: 'u1',
        filename: 'test.csv',
        s3_uri: 's3://b/k',
        columns: ['a'],
        row_count: 1,
      }]);
    });
  });
});

// ---------------------------------------------------------------------------
// Test 6: Phase badge shown in conversation header (covers line 180-181)
// ---------------------------------------------------------------------------
describe('ReconcilePage - phase display', () => {
  it('shows phase badge with current phase name', async () => {
    mockFetch.mockResolvedValueOnce(makeChatResponse({ phase: 'intent' }));

    render(<ReconcilePage />);
    await startConversation();

    expect(screen.getByText('intent')).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// Test 7: Does not send empty message (covers line 106)
// ---------------------------------------------------------------------------
describe('ReconcilePage - empty input guard', () => {
  it('does not send when input is empty', async () => {
    mockFetch.mockResolvedValueOnce(makeChatResponse());

    render(<ReconcilePage />);
    await startConversation();

    // Send button should be disabled when input is empty
    const sendBtn = screen.getByLabelText('Send');
    expect(sendBtn).toBeDisabled();

    // Only the initial fetch should have been called
    expect(mockFetch).toHaveBeenCalledTimes(1);
  });
});

// ---------------------------------------------------------------------------
// Test 8: File upload via hidden input (covers handleFileInputChange, handleFileSelect)
// ---------------------------------------------------------------------------
describe('ReconcilePage - file upload via input', () => {
  it('uploads file and shows pill after conversation starts', async () => {
    const { uploadFile: mockUpload } = require('@/lib/upload-client');
    const attachment = {
      upload_id: 'u1', filename: 'data.csv', s3_uri: 's3://b/k',
      columns: ['a', 'b'], row_count: 10,
    };
    mockUpload.mockResolvedValueOnce(attachment);

    mockFetch.mockResolvedValueOnce(makeChatResponse());

    render(<ReconcilePage />);
    await startConversation();

    // Click the attach button to trigger hidden file input
    const attachBtn = screen.getByLabelText('Attach file');
    expect(attachBtn).toBeInTheDocument();

    // Simulate file selection via hidden input
    const fileInput = document.querySelector('input[type="file"]') as HTMLInputElement;
    const file = new File(['a,b\n1,2'], 'data.csv', { type: 'text/csv' });

    await act(async () => {
      fireEvent.change(fileInput, { target: { files: [file] } });
    });

    // Pill should appear
    await waitFor(() => {
      expect(screen.getByTestId('file-pill')).toBeInTheDocument();
    });
  });

  it('removes file pill when remove button clicked', async () => {
    const { uploadFile: mockUpload } = require('@/lib/upload-client');
    mockUpload.mockResolvedValueOnce({
      upload_id: 'u1', filename: 'data.csv', s3_uri: 's3://b/k',
      columns: ['a'], row_count: 1,
    });

    mockFetch.mockResolvedValueOnce(makeChatResponse());

    render(<ReconcilePage />);
    await startConversation();

    const fileInput = document.querySelector('input[type="file"]') as HTMLInputElement;
    const file = new File(['a\n1'], 'data.csv', { type: 'text/csv' });
    await act(async () => {
      fireEvent.change(fileInput, { target: { files: [file] } });
    });

    await waitFor(() => {
      expect(screen.getByTestId('file-pill')).toBeInTheDocument();
    });

    // Click remove
    await act(async () => {
      fireEvent.click(screen.getByTestId('remove-file'));
    });

    await waitFor(() => {
      expect(screen.queryByTestId('file-pill')).not.toBeInTheDocument();
    });
  });
});

// ---------------------------------------------------------------------------
// Test 9: Drag and drop (covers handleDragOver, handleDragLeave, handleDrop)
// ---------------------------------------------------------------------------
describe('ReconcilePage - drag and drop', () => {
  it('handles drag over, drag leave, and drop events', async () => {
    const { uploadFile: mockUpload } = require('@/lib/upload-client');
    mockUpload.mockResolvedValueOnce({
      upload_id: 'u1', filename: 'dropped.csv', s3_uri: 's3://b/k',
      columns: ['x'], row_count: 1,
    });

    mockFetch.mockResolvedValueOnce(makeChatResponse());

    const { container } = render(<ReconcilePage />);
    await startConversation();

    // The main container has drag handlers
    const mainDiv = container.firstChild as HTMLElement;

    // Drag over
    await act(async () => {
      fireEvent.dragOver(mainDiv, { preventDefault: jest.fn() });
    });

    // Drag leave
    await act(async () => {
      fireEvent.dragLeave(mainDiv);
    });

    // Drop
    const file = new File(['x\n1'], 'dropped.csv', { type: 'text/csv' });
    await act(async () => {
      fireEvent.drop(mainDiv, {
        preventDefault: jest.fn(),
        dataTransfer: { files: [file] },
      });
    });

    // Should trigger upload
    await waitFor(() => {
      expect(mockUpload).toHaveBeenCalled();
    });
  });
});

// ---------------------------------------------------------------------------
// Test 10: Submit with attached files (covers handleSubmit with files)
// ---------------------------------------------------------------------------
describe('ReconcilePage - submit with files', () => {
  it('sends completed file attachments with message', async () => {
    const { uploadFile: mockUpload } = require('@/lib/upload-client');
    const attachment = {
      upload_id: 'u1', filename: 'data.csv', s3_uri: 's3://b/k',
      columns: ['a'], row_count: 1,
    };
    mockUpload.mockResolvedValueOnce(attachment);

    // Start conversation
    mockFetch.mockResolvedValueOnce(makeChatResponse());
    // Response for message with files
    mockFetch.mockResolvedValueOnce(makeChatResponse({
      message: {
        role: 'agent',
        segments: [{ type: 'text', content: 'Got the file!' }],
        timestamp: new Date().toISOString(),
      },
    }));

    render(<ReconcilePage />);
    await startConversation();

    // Upload a file
    const fileInput = document.querySelector('input[type="file"]') as HTMLInputElement;
    const file = new File(['a\n1'], 'data.csv', { type: 'text/csv' });
    await act(async () => {
      fireEvent.change(fileInput, { target: { files: [file] } });
    });

    await waitFor(() => {
      expect(screen.getByTestId('file-pill')).toBeInTheDocument();
    });

    // Type message and submit
    const user = userEvent.setup();
    const input = screen.getByPlaceholderText('Type your message...');
    await user.type(input, 'here is my file{enter}');

    // Verify files were included in the request
    await waitFor(() => {
      expect(mockFetch).toHaveBeenCalledTimes(2);
      const body = JSON.parse(mockFetch.mock.calls[1][1].body);
      expect(body.files).toEqual([attachment]);
    });
  });
});

