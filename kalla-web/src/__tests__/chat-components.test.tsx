import React from 'react';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import { ChatMessage } from '@/components/chat/ChatMessage';
import { MatchProposalCard } from '@/components/chat/MatchProposalCard';
import { FieldPreview } from '@/components/FieldPreview';
import { LiveProgressIndicator } from '@/components/LiveProgressIndicator';
import type { ChatMessage as ChatMessageType } from '@/lib/chat-types';

// ---------------------------------------------------------------------------
// Mock @/lib/api for LiveProgressIndicator
// ---------------------------------------------------------------------------
jest.mock('@/lib/api', () => ({
  getRun: jest.fn(),
}));
import { getRun } from '@/lib/api';
const mockGetRun = getRun as jest.MockedFunction<typeof getRun>;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function makeMessage(
  role: 'agent' | 'user',
  text: string,
  extra?: Partial<ChatMessageType>,
): ChatMessageType {
  return {
    role,
    segments: [{ type: 'text', content: text }],
    timestamp: '2025-01-15T10:30:00Z',
    ...extra,
  };
}

// ---------------------------------------------------------------------------
// ChatMessage
// ---------------------------------------------------------------------------
describe('ChatMessage', () => {
  it('renders user message with data-testid="user-message"', () => {
    render(<ChatMessage message={makeMessage('user', 'hello')} />);
    expect(screen.getByTestId('user-message')).toBeInTheDocument();
  });

  it('renders agent message with data-testid="agent-message"', () => {
    render(<ChatMessage message={makeMessage('agent', 'hi there')} />);
    expect(screen.getByTestId('agent-message')).toBeInTheDocument();
  });

  it('renders text content for user messages as plain text (not markdown)', () => {
    render(<ChatMessage message={makeMessage('user', 'plain text message')} />);
    expect(screen.getByText('plain text message')).toBeInTheDocument();
    // User messages should NOT be wrapped with MarkdownRenderer's <div class="text-sm">
    const el = screen.getByText('plain text message');
    // The direct parent should not be a <p> (which the mock ReactMarkdown would produce)
    expect(el.tagName).not.toBe('P');
  });

  it('renders text content for agent messages using MarkdownRenderer (content visible)', () => {
    render(<ChatMessage message={makeMessage('agent', 'agent reply')} />);
    expect(screen.getByText('agent reply')).toBeInTheDocument();
  });

  it('renders timestamp', () => {
    render(<ChatMessage message={makeMessage('user', 'hi')} />);
    // toLocaleTimeString will produce something like "10:30:00 AM" depending on locale
    const timeEl = screen.getByText(
      (_, el) =>
        el?.tagName === 'SPAN' &&
        el?.textContent !== '' &&
        /\d{1,2}:\d{2}/.test(el?.textContent ?? ''),
    );
    expect(timeEl).toBeInTheDocument();
  });

  it('renders match_proposal card segments using MatchProposalCard', () => {
    const message: ChatMessageType = {
      role: 'agent',
      segments: [
        {
          type: 'card',
          card_type: 'match_proposal',
          card_id: 'mp-1',
          data: {
            left: { name: 'Alice' },
            right: { name: 'Bob' },
            reasoning: 'Names similar',
          },
        },
      ],
      timestamp: '2025-01-15T10:30:00Z',
    };
    render(<ChatMessage message={message} />);
    expect(screen.getByText('Match Proposal')).toBeInTheDocument();
  });

  it('returns null for unknown segment types', () => {
    const message: ChatMessageType = {
      role: 'agent',
      segments: [
        { type: 'card', card_type: 'select', card_id: 'x', data: {} },
      ],
      timestamp: '2025-01-15T10:30:00Z',
    };
    const { container } = render(<ChatMessage message={message} />);
    // The segment area should have no card content, just the avatar + timestamp wrapper
    expect(container.querySelector('[data-testid="agent-message"]')).toBeInTheDocument();
    // No card-specific text rendered
    expect(screen.queryByText('Match Proposal')).not.toBeInTheDocument();
  });

  it('shows "K" avatar for agent, "U" avatar for user', () => {
    const { unmount } = render(<ChatMessage message={makeMessage('agent', 'a')} />);
    expect(screen.getByText('K')).toBeInTheDocument();
    unmount();

    render(<ChatMessage message={makeMessage('user', 'u')} />);
    expect(screen.getByText('U')).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// MatchProposalCard
// ---------------------------------------------------------------------------
describe('MatchProposalCard', () => {
  const defaultData = {
    left: { id: '1', name: 'Alice', email: 'alice@example.com' },
    right: { id: '2', name: 'Bob', email: 'bob@example.com' },
    reasoning: 'Email domains match and names are similar.',
  };

  it('renders "Match Proposal" title', () => {
    render(<MatchProposalCard cardId="c1" data={defaultData} />);
    expect(screen.getByText('Match Proposal')).toBeInTheDocument();
  });

  it('renders left source data fields', () => {
    render(<MatchProposalCard cardId="c1" data={defaultData} />);
    expect(screen.getByText('Left Source')).toBeInTheDocument();
    expect(screen.getByText('Alice')).toBeInTheDocument();
  });

  it('renders right source data fields', () => {
    render(<MatchProposalCard cardId="c1" data={defaultData} />);
    expect(screen.getByText('Right Source')).toBeInTheDocument();
    expect(screen.getByText('Bob')).toBeInTheDocument();
  });

  it('renders reasoning text when provided', () => {
    render(<MatchProposalCard cardId="c1" data={defaultData} />);
    expect(
      screen.getByText('Email domains match and names are similar.'),
    ).toBeInTheDocument();
  });

  it('shows action buttons before response', () => {
    render(<MatchProposalCard cardId="c1" data={defaultData} />);
    expect(screen.getByText('Yes, match')).toBeInTheDocument();
    expect(screen.getByText('No')).toBeInTheDocument();
    expect(screen.getByText('Not sure')).toBeInTheDocument();
  });

  it('clicking "Yes, match" calls onAction with confirm and row data', () => {
    const onAction = jest.fn();
    render(
      <MatchProposalCard cardId="c1" data={defaultData} onAction={onAction} />,
    );
    fireEvent.click(screen.getByText('Yes, match'));
    expect(onAction).toHaveBeenCalledWith('c1', 'confirm', {
      left: defaultData.left,
      right: defaultData.right,
    });
  });

  it('clicking "No" calls onAction with reject', () => {
    const onAction = jest.fn();
    render(
      <MatchProposalCard cardId="c1" data={defaultData} onAction={onAction} />,
    );
    fireEvent.click(screen.getByText('No'));
    expect(onAction).toHaveBeenCalledWith('c1', 'reject', undefined);
  });

  it('clicking "Not sure" calls onAction with unsure', () => {
    const onAction = jest.fn();
    render(
      <MatchProposalCard cardId="c1" data={defaultData} onAction={onAction} />,
    );
    fireEvent.click(screen.getByText('Not sure'));
    expect(onAction).toHaveBeenCalledWith('c1', 'unsure', undefined);
  });

  it('after clicking "Yes, match", shows Confirmed badge and hides buttons', () => {
    render(<MatchProposalCard cardId="c1" data={defaultData} />);
    fireEvent.click(screen.getByText('Yes, match'));
    expect(screen.getByText('Confirmed')).toBeInTheDocument();
    expect(screen.queryByText('Yes, match')).not.toBeInTheDocument();
    expect(screen.queryByText('No')).not.toBeInTheDocument();
    expect(screen.queryByText('Not sure')).not.toBeInTheDocument();
  });

  it('after clicking "No", shows Rejected badge and hides buttons', () => {
    render(<MatchProposalCard cardId="c1" data={defaultData} />);
    fireEvent.click(screen.getByText('No'));
    expect(screen.getByText('Rejected')).toBeInTheDocument();
    expect(screen.queryByText('Yes, match')).not.toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// FieldPreview
// ---------------------------------------------------------------------------
describe('FieldPreview', () => {
  const mockFetch = jest.fn();

  beforeEach(() => {
    mockFetch.mockReset();
    global.fetch = mockFetch;
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  const previewResponse = {
    alias: 'customers',
    columns: [
      { name: 'id', data_type: 'integer', nullable: false },
      { name: 'name', data_type: 'text', nullable: false },
      { name: 'email', data_type: 'text', nullable: true },
    ],
    rows: [
      ['1', 'Alice', 'alice@example.com'],
      ['2', 'Bob', 'null'],
    ],
    total_rows: 100,
    preview_rows: 2,
  };

  it('shows loading state initially', () => {
    mockFetch.mockReturnValue(new Promise(() => {})); // never resolves
    render(<FieldPreview sourceAlias="customers" />);
    expect(screen.getByText(/Loading fields for customers/)).toBeInTheDocument();
  });

  it('after fetch, shows column names in table', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => previewResponse,
    });
    render(<FieldPreview sourceAlias="customers" />);
    await waitFor(() => {
      expect(screen.getByText('id')).toBeInTheDocument();
      expect(screen.getByText('name')).toBeInTheDocument();
      expect(screen.getByText('email')).toBeInTheDocument();
    });
  });

  it('shows data types for each column', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => previewResponse,
    });
    render(<FieldPreview sourceAlias="customers" />);
    await waitFor(() => {
      expect(screen.getByText('integer')).toBeInTheDocument();
      expect(screen.getAllByText('text').length).toBeGreaterThanOrEqual(2);
    });
  });

  it('shows sample values from rows', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => previewResponse,
    });
    render(<FieldPreview sourceAlias="customers" />);
    await waitFor(() => {
      expect(screen.getByText('Alice')).toBeInTheDocument();
      expect(screen.getByText('alice@example.com')).toBeInTheDocument();
    });
  });

  it('shows "nullable" badge for nullable columns', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => previewResponse,
    });
    render(<FieldPreview sourceAlias="customers" />);
    await waitFor(() => {
      expect(screen.getByText('nullable')).toBeInTheDocument();
    });
  });

  it('shows column count', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => previewResponse,
    });
    render(<FieldPreview sourceAlias="customers" />);
    await waitFor(() => {
      expect(screen.getByTestId('field-count')).toHaveTextContent('3 columns');
    });
  });

  it('shows error message on fetch failure', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      text: async () => 'Source not found',
    });
    render(<FieldPreview sourceAlias="customers" />);
    await waitFor(() => {
      expect(screen.getByText(/Source not found/)).toBeInTheDocument();
    });
  });

  it('shows null values in italic style', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => previewResponse,
    });
    render(<FieldPreview sourceAlias="customers" />);
    await waitFor(() => {
      const nullEl = screen.getByText('null');
      expect(nullEl.tagName).toBe('SPAN');
      expect(nullEl).toHaveClass('italic');
    });
  });
});

// ---------------------------------------------------------------------------
// LiveProgressIndicator
// ---------------------------------------------------------------------------
describe('LiveProgressIndicator', () => {
  beforeEach(() => {
    jest.useFakeTimers();
    mockGetRun.mockReset();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  const baseRun = {
    run_id: 'run-1',
    recipe_id: 'rec-1',
    started_at: '2025-01-15T10:00:00Z',
    left_source: 'customers',
    right_source: 'invoices',
    left_record_count: 100,
    right_record_count: 200,
    matched_count: 0,
    unmatched_left_count: 100,
    unmatched_right_count: 200,
    status: 'Running' as const,
  };

  it('shows loading state initially', async () => {
    mockGetRun.mockReturnValue(new Promise(() => {})); // never resolves
    await act(async () => {
      render(<LiveProgressIndicator runId="run-1" />);
    });
    expect(screen.getByTestId('progress-spinner')).toBeInTheDocument();
    expect(screen.getByText('Loading run status...')).toBeInTheDocument();
  });

  it('shows running state with spinner after fetch returns Running status', async () => {
    mockGetRun.mockResolvedValue({ ...baseRun, status: 'Running' });
    await act(async () => {
      render(<LiveProgressIndicator runId="run-1" />);
    });
    await waitFor(() => {
      expect(screen.getByTestId('progress-spinner')).toBeInTheDocument();
      expect(screen.getByTestId('progress-status')).toHaveTextContent(
        'Running reconciliation...',
      );
    });
  });

  it('shows "Running reconciliation..." text when running', async () => {
    mockGetRun.mockResolvedValue({ ...baseRun, status: 'Running' });
    await act(async () => {
      render(<LiveProgressIndicator runId="run-1" />);
    });
    await waitFor(() => {
      expect(screen.getByText('Running reconciliation...')).toBeInTheDocument();
    });
  });

  it('shows matched count badge when running and matched_count > 0', async () => {
    mockGetRun.mockResolvedValue({
      ...baseRun,
      status: 'Running',
      matched_count: 42,
    });
    await act(async () => {
      render(<LiveProgressIndicator runId="run-1" />);
    });
    await waitFor(() => {
      expect(screen.getByText('42 matched')).toBeInTheDocument();
    });
  });

  it('shows completed state with CheckCircle after fetch returns Completed', async () => {
    mockGetRun.mockResolvedValue({
      ...baseRun,
      status: 'Completed',
      matched_count: 75,
      completed_at: '2025-01-15T10:05:00Z',
    });
    await act(async () => {
      render(<LiveProgressIndicator runId="run-1" />);
    });
    await waitFor(() => {
      expect(screen.getByTestId('progress-status')).toHaveTextContent(
        'Reconciliation completed',
      );
    });
  });

  it('shows "Reconciliation completed" text when completed', async () => {
    mockGetRun.mockResolvedValue({
      ...baseRun,
      status: 'Completed',
      matched_count: 75,
      completed_at: '2025-01-15T10:05:00Z',
    });
    await act(async () => {
      render(<LiveProgressIndicator runId="run-1" />);
    });
    await waitFor(() => {
      expect(
        screen.getByText('Reconciliation completed'),
      ).toBeInTheDocument();
    });
  });

  it('shows failed state when status is Failed', async () => {
    mockGetRun.mockResolvedValue({ ...baseRun, status: 'Failed' });
    await act(async () => {
      render(<LiveProgressIndicator runId="run-1" />);
    });
    await waitFor(() => {
      expect(screen.getByTestId('progress-status')).toHaveTextContent(
        'Reconciliation failed',
      );
    });
  });

  it('calls onComplete when status changes to Completed', async () => {
    const completedRun = {
      ...baseRun,
      status: 'Completed' as const,
      matched_count: 75,
      completed_at: '2025-01-15T10:05:00Z',
    };

    // First call returns Running, second returns Completed
    mockGetRun.mockResolvedValueOnce({ ...baseRun, status: 'Running' });
    mockGetRun.mockResolvedValueOnce(completedRun);

    const onComplete = jest.fn();

    await act(async () => {
      render(<LiveProgressIndicator runId="run-1" onComplete={onComplete} />);
    });

    // Wait for the initial Running state to render
    await waitFor(() => {
      expect(screen.getByText('Running reconciliation...')).toBeInTheDocument();
    });

    // Advance timers to trigger the next poll
    await act(async () => {
      jest.advanceTimersByTime(2000);
    });

    await waitFor(() => {
      expect(onComplete).toHaveBeenCalledWith(completedRun);
    });
  });

  it('calls onComplete when status changes to Failed', async () => {
    const failedRun = { ...baseRun, status: 'Failed' as const };

    mockGetRun.mockResolvedValueOnce({ ...baseRun, status: 'Running' });
    mockGetRun.mockResolvedValueOnce(failedRun);

    const onComplete = jest.fn();

    await act(async () => {
      render(<LiveProgressIndicator runId="run-1" onComplete={onComplete} />);
    });

    await waitFor(() => {
      expect(screen.getByText('Running reconciliation...')).toBeInTheDocument();
    });

    await act(async () => {
      jest.advanceTimersByTime(2000);
    });

    await waitFor(() => {
      expect(onComplete).toHaveBeenCalledWith(failedRun);
    });
  });

  it('shows error message when fetch fails', async () => {
    mockGetRun.mockRejectedValue(new Error('Network error'));
    await act(async () => {
      render(<LiveProgressIndicator runId="run-1" />);
    });
    await waitFor(() => {
      expect(screen.getByText('Network error')).toBeInTheDocument();
    });
  });
});
