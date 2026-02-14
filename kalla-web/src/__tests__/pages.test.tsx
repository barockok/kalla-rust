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
const mockPathname = jest.fn().mockReturnValue('/');
const mockParams = jest.fn().mockReturnValue({ id: 'test-run-id' });
jest.mock('next/navigation', () => ({
  usePathname: () => mockPathname(),
  useParams: () => mockParams(),
}));

// Mock @tanstack/react-query
const mockUseQuery = jest.fn();
jest.mock('@tanstack/react-query', () => ({
  useQuery: (...args: unknown[]) => mockUseQuery(...args),
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
import { listSources, registerSource, validateRecipe } from '@/lib/api';
const mockListSources = listSources as jest.MockedFunction<typeof listSources>;
const mockRegisterSource = registerSource as jest.MockedFunction<typeof registerSource>;
const mockValidateRecipe = validateRecipe as jest.MockedFunction<typeof validateRecipe>;

// Mock SourcePreview component (it does internal fetches)
jest.mock('@/components/SourcePreview', () => ({
  SourcePreview: ({ sourceAlias }: { sourceAlias: string }) => (
    <div data-testid="source-preview">Preview for {sourceAlias}</div>
  ),
}));

// Mock PrimaryKeyConfirmation component (it does internal fetches)
jest.mock('@/components/PrimaryKeyConfirmation', () => ({
  PrimaryKeyConfirmation: ({ sourceAlias }: { sourceAlias: string }) => (
    <div data-testid="pk-confirmation">PK for {sourceAlias}</div>
  ),
}));

// Mock chat components
jest.mock('@/components/chat/ChatMessage', () => ({
  ChatMessage: ({ message }: { message: { role: string; segments: Array<{ content?: string }> } }) => (
    <div data-testid={message.role === 'agent' ? 'agent-message' : 'user-message'}>
      {message.segments.map((s: { content?: string }, i: number) => (
        <span key={i}>{s.content}</span>
      ))}
    </div>
  ),
}));

jest.mock('@/components/chat/RecipeCard', () => ({
  RecipeCard: ({ recipe }: { recipe: unknown }) => (
    <div data-testid="recipe-card">{recipe ? 'Has recipe' : 'No recipe'}</div>
  ),
}));

// Mock fetch
const mockFetch = jest.fn();
global.fetch = mockFetch;

// Ensure navigator.clipboard exists in jsdom for spyOn
if (!navigator.clipboard) {
  Object.defineProperty(navigator, 'clipboard', {
    value: { writeText: jest.fn().mockResolvedValue(undefined) },
    writable: true,
    configurable: true,
  });
}

// Import pages after mocks are set up
import Home from '@/app/page';
import SourcesPage from '@/app/sources/page';
import RunsPage from '@/app/runs/page';
import RunDetailPage from '@/app/runs/[id]/page';
import RecipesPage from '@/app/recipes/page';
import ReconcilePage from '@/app/reconcile/page';

beforeEach(() => {
  jest.clearAllMocks();
  mockFetch.mockReset();
  mockUseQuery.mockReset();
  mockListSources.mockReset();
  mockRegisterSource.mockReset();
  mockValidateRecipe.mockReset();
});

// ---------------------------------------------------------------------------
// 1. Home Page
// ---------------------------------------------------------------------------
describe('Home Page', () => {
  it('renders "Welcome to Kalla" heading', () => {
    render(<Home />);
    expect(screen.getByText('Welcome to Kalla')).toBeInTheDocument();
  });

  it('renders "Universal Reconciliation Engine" description', () => {
    render(<Home />);
    expect(
      screen.getByText(/Universal Reconciliation Engine/)
    ).toBeInTheDocument();
  });

  it('shows Quick Start card with "New Reconciliation" button/link', () => {
    render(<Home />);
    expect(screen.getByText('Quick Start')).toBeInTheDocument();
    const link = screen.getByText('New Reconciliation').closest('a');
    expect(link).toHaveAttribute('href', '/reconcile');
  });

  it('shows Data Sources card with "Manage Sources" link', () => {
    render(<Home />);
    expect(screen.getByText('Data Sources')).toBeInTheDocument();
    const link = screen.getByText('Manage Sources').closest('a');
    expect(link).toHaveAttribute('href', '/sources');
  });

  it('shows Run History card with "View History" link', () => {
    render(<Home />);
    expect(screen.getByText('Run History')).toBeInTheDocument();
    const link = screen.getByText('View History').closest('a');
    expect(link).toHaveAttribute('href', '/runs');
  });

  it('shows Recipes card with "View Recipes" link', () => {
    render(<Home />);
    expect(screen.getByText('Recipes')).toBeInTheDocument();
    const link = screen.getByText('View Recipes').closest('a');
    expect(link).toHaveAttribute('href', '/recipes');
  });

  it('shows "How It Works" section with 3 steps', () => {
    render(<Home />);
    expect(screen.getByText('How It Works')).toBeInTheDocument();
    expect(screen.getByText('Connect Your Data')).toBeInTheDocument();
    expect(screen.getByText('Describe Your Match')).toBeInTheDocument();
    expect(screen.getByText('Review & Execute')).toBeInTheDocument();
  });

  it('shows "Features" section with feature list', () => {
    render(<Home />);
    expect(screen.getByText('Features')).toBeInTheDocument();
    expect(screen.getByText(/TB-scale data processing/)).toBeInTheDocument();
    expect(screen.getByText(/Financial tolerance matching/)).toBeInTheDocument();
    expect(screen.getByText(/1:1, 1:N, and M:1 match patterns/)).toBeInTheDocument();
    expect(screen.getByText(/AI-powered recipe generation/)).toBeInTheDocument();
    expect(screen.getByText(/Parquet evidence store/)).toBeInTheDocument();
    expect(screen.getByText(/Human-in-the-loop/)).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// 2. Sources Page
// ---------------------------------------------------------------------------
describe('Sources Page', () => {
  it('renders "Data Sources" heading', async () => {
    mockListSources.mockResolvedValue([]);
    await act(async () => {
      render(<SourcesPage />);
    });
    expect(screen.getByText('Data Sources')).toBeInTheDocument();
  });

  it('shows loading state initially', () => {
    // Make listSources never resolve so loading persists
    mockListSources.mockReturnValue(new Promise(() => {}));
    render(<SourcesPage />);
    expect(screen.getByText('Loading sources...')).toBeInTheDocument();
  });

  it('shows "No sources registered yet" when no sources', async () => {
    mockListSources.mockResolvedValue([]);
    await act(async () => {
      render(<SourcesPage />);
    });
    await waitFor(() => {
      expect(screen.getByText('No sources registered yet')).toBeInTheDocument();
    });
  });

  it('shows registered sources after fetch completes', async () => {
    mockListSources.mockResolvedValue([
      { alias: 'invoices', uri: 'file://invoices.csv', source_type: 'csv', status: 'connected' },
      { alias: 'payments', uri: 'postgres://localhost/db', source_type: 'postgres', status: 'connected' },
    ]);
    await act(async () => {
      render(<SourcesPage />);
    });
    await waitFor(() => {
      expect(screen.getByText('invoices')).toBeInTheDocument();
      expect(screen.getByText('payments')).toBeInTheDocument();
    });
  });

  it('shows source alias and URI for each source', async () => {
    mockListSources.mockResolvedValue([
      { alias: 'invoices', uri: 'file://invoices.csv', source_type: 'csv', status: 'connected' },
    ]);
    await act(async () => {
      render(<SourcesPage />);
    });
    await waitFor(() => {
      expect(screen.getByText('invoices')).toBeInTheDocument();
      expect(screen.getByText('file://invoices.csv')).toBeInTheDocument();
    });
  });

  it('shows type badge and status badge', async () => {
    mockListSources.mockResolvedValue([
      { alias: 'invoices', uri: 'file://invoices.csv', source_type: 'csv', status: 'connected' },
      { alias: 'payments', uri: 'postgres://localhost/db', source_type: 'postgres', status: 'error' },
    ]);
    await act(async () => {
      render(<SourcesPage />);
    });
    await waitFor(() => {
      expect(screen.getByText('csv')).toBeInTheDocument();
      expect(screen.getByText('postgres')).toBeInTheDocument();
      expect(screen.getByText('connected')).toBeInTheDocument();
      expect(screen.getByText('error')).toBeInTheDocument();
    });
  });

  it('has file upload tab and connection string tab', async () => {
    mockListSources.mockResolvedValue([]);
    await act(async () => {
      render(<SourcesPage />);
    });
    expect(screen.getByText('Upload File')).toBeInTheDocument();
    expect(screen.getByText('Connection String')).toBeInTheDocument();
  });

  it('shows error alert when adding source fails', async () => {
    const user = userEvent.setup();
    mockListSources.mockResolvedValue([]);
    mockRegisterSource.mockRejectedValue(new Error('Connection refused'));

    await act(async () => {
      render(<SourcesPage />);
    });

    // Switch to connection string tab using userEvent (Radix tabs need full pointer events)
    const connectionTab = screen.getByRole('tab', { name: /Connection String/i });
    await user.click(connectionTab);

    // Find the alias and URI inputs
    await waitFor(() => {
      expect(screen.getByLabelText('Alias')).toBeInTheDocument();
    });

    const aliasInput = screen.getByLabelText('Alias');
    const uriInput = screen.getByLabelText('URI');
    await user.clear(aliasInput);
    await user.type(aliasInput, 'test-source');
    await user.clear(uriInput);
    await user.type(uriInput, 'postgres://localhost/test');

    // Click add source button
    await user.click(screen.getByText('Add Source'));

    await waitFor(() => {
      expect(screen.getByText('Connection refused')).toBeInTheDocument();
    });
  });

  it('shows success alert when adding source succeeds', async () => {
    const user = userEvent.setup();
    mockListSources.mockResolvedValue([]);
    mockRegisterSource.mockResolvedValue({ success: true, message: 'Source registered' });

    await act(async () => {
      render(<SourcesPage />);
    });

    // Switch to connection string tab using userEvent
    const connectionTab = screen.getByRole('tab', { name: /Connection String/i });
    await user.click(connectionTab);

    await waitFor(() => {
      expect(screen.getByLabelText('Alias')).toBeInTheDocument();
    });

    const aliasInput = screen.getByLabelText('Alias');
    const uriInput = screen.getByLabelText('URI');
    await user.clear(aliasInput);
    await user.type(aliasInput, 'my-source');
    await user.clear(uriInput);
    await user.type(uriInput, 'postgres://localhost/db');

    // Click add source button
    await user.click(screen.getByText('Add Source'));

    await waitFor(() => {
      expect(screen.getByText(/Source "my-source" connected successfully/)).toBeInTheDocument();
    });
  });
});

// ---------------------------------------------------------------------------
// 3. Runs Page
// ---------------------------------------------------------------------------
describe('Runs Page', () => {
  it('renders "Run History" heading', () => {
    mockUseQuery.mockReturnValue({ data: [], isLoading: false, refetch: jest.fn(), isRefetching: false });
    render(<RunsPage />);
    expect(screen.getByText('Run History')).toBeInTheDocument();
  });

  it('shows loading state when isLoading is true', () => {
    mockUseQuery.mockReturnValue({ data: undefined, isLoading: true, refetch: jest.fn(), isRefetching: false });
    render(<RunsPage />);
    expect(screen.getByText('Loading runs...')).toBeInTheDocument();
  });

  it('shows "No reconciliation runs yet" when runs is empty', () => {
    mockUseQuery.mockReturnValue({ data: [], isLoading: false, refetch: jest.fn(), isRefetching: false });
    render(<RunsPage />);
    expect(screen.getByText('No reconciliation runs yet')).toBeInTheDocument();
  });

  it('shows run table with correct columns when runs have data', () => {
    mockUseQuery.mockReturnValue({
      data: [
        {
          run_id: 'abcdef12-3456-7890-abcd-ef1234567890',
          recipe_id: 'invoice-match',
          status: 'Completed',
          started_at: '2025-01-15T10:00:00Z',
          matched_count: 100,
          unmatched_left_count: 5,
          unmatched_right_count: 3,
        },
      ],
      isLoading: false,
      refetch: jest.fn(),
      isRefetching: false,
    });
    render(<RunsPage />);

    expect(screen.getByText('Run ID')).toBeInTheDocument();
    expect(screen.getByText('Recipe')).toBeInTheDocument();
    expect(screen.getByText('Status')).toBeInTheDocument();
    expect(screen.getByText('Matched')).toBeInTheDocument();
    expect(screen.getByText('Left Orphans')).toBeInTheDocument();
    expect(screen.getByText('Right Orphans')).toBeInTheDocument();
    expect(screen.getByText('Started')).toBeInTheDocument();

    // Verify actual data renders
    expect(screen.getByText('abcdef12...')).toBeInTheDocument();
    expect(screen.getByText('invoice-match')).toBeInTheDocument();
    expect(screen.getByText('100')).toBeInTheDocument();
    expect(screen.getByText('5')).toBeInTheDocument();
    expect(screen.getByText('3')).toBeInTheDocument();
  });

  it('shows status badges (Completed=green, Running=blue, Failed=destructive)', () => {
    mockUseQuery.mockReturnValue({
      data: [
        { run_id: 'run-1-xxxxxxxx', recipe_id: 'r1', status: 'Completed', started_at: '2025-01-15T10:00:00Z', matched_count: 10, unmatched_left_count: 0, unmatched_right_count: 0 },
        { run_id: 'run-2-xxxxxxxx', recipe_id: 'r2', status: 'Running', started_at: '2025-01-15T11:00:00Z', matched_count: 0, unmatched_left_count: 0, unmatched_right_count: 0 },
        { run_id: 'run-3-xxxxxxxx', recipe_id: 'r3', status: 'Failed', started_at: '2025-01-15T12:00:00Z', matched_count: 0, unmatched_left_count: 0, unmatched_right_count: 0 },
      ],
      isLoading: false,
      refetch: jest.fn(),
      isRefetching: false,
    });
    render(<RunsPage />);

    const completedBadge = screen.getByText('Completed');
    expect(completedBadge).toHaveClass('bg-green-500');

    const runningBadge = screen.getByText('Running');
    expect(runningBadge).toHaveClass('bg-blue-500');

    // Failed uses variant="destructive" on the Badge
    expect(screen.getByText('Failed')).toBeInTheDocument();
  });

  it('shows refresh button', () => {
    mockUseQuery.mockReturnValue({ data: [], isLoading: false, refetch: jest.fn(), isRefetching: false });
    render(<RunsPage />);
    expect(screen.getByText('Refresh')).toBeInTheDocument();
  });

  it('shows "New Reconciliation" link when no runs', () => {
    mockUseQuery.mockReturnValue({ data: [], isLoading: false, refetch: jest.fn(), isRefetching: false });
    render(<RunsPage />);
    const link = screen.getByText('New Reconciliation').closest('a');
    expect(link).toHaveAttribute('href', '/reconcile');
  });
});

// ---------------------------------------------------------------------------
// 4. Run Detail Page
// ---------------------------------------------------------------------------
describe('Run Detail Page', () => {
  const mockRunData = {
    run_id: 'test-run-id',
    recipe_id: 'invoice-payment-match',
    started_at: '2025-01-15T10:00:00Z',
    completed_at: '2025-01-15T10:05:00Z',
    left_source: 'file://invoices.csv',
    right_source: 'file://payments.csv',
    left_record_count: 1000,
    right_record_count: 950,
    matched_count: 900,
    unmatched_left_count: 100,
    unmatched_right_count: 50,
    status: 'Completed',
  };

  it('renders "Run Details" heading', () => {
    mockUseQuery.mockReturnValue({ data: mockRunData, isLoading: false, error: null });
    render(<RunDetailPage />);
    expect(screen.getByText('Run Details')).toBeInTheDocument();
  });

  it('shows loading state', () => {
    mockUseQuery.mockReturnValue({ data: undefined, isLoading: true, error: null });
    render(<RunDetailPage />);
    expect(screen.getByText('Loading run details...')).toBeInTheDocument();
  });

  it('shows error state when run fails to load', () => {
    mockUseQuery.mockReturnValue({ data: undefined, isLoading: false, error: new Error('Not found') });
    render(<RunDetailPage />);
    expect(screen.getByText('Failed to load run details')).toBeInTheDocument();
  });

  it('shows run metadata (recipe_id, match rate, dates, sources)', () => {
    mockUseQuery.mockReturnValue({ data: mockRunData, isLoading: false, error: null });
    render(<RunDetailPage />);

    expect(screen.getByText('Recipe ID')).toBeInTheDocument();
    expect(screen.getByText('invoice-payment-match')).toBeInTheDocument();
    expect(screen.getByText('Match Rate')).toBeInTheDocument();
    // match rate = 900 / (900+100) * 100 = 90.0%
    expect(screen.getByText('90.0%')).toBeInTheDocument();
    expect(screen.getByText('file://invoices.csv')).toBeInTheDocument();
    expect(screen.getByText('file://payments.csv')).toBeInTheDocument();
  });

  it('shows status card with badge', () => {
    mockUseQuery.mockReturnValue({ data: mockRunData, isLoading: false, error: null });
    const { container } = render(<RunDetailPage />);

    // There should be at least one "Status" label (the card title)
    const statusLabels = screen.getAllByText('Status');
    expect(statusLabels.length).toBeGreaterThanOrEqual(1);

    // The Completed badge has data-slot="badge" and bg-green-500
    const badges = container.querySelectorAll('[data-slot="badge"]');
    const completedBadge = Array.from(badges).find(b => b.textContent === 'Completed');
    expect(completedBadge).toBeTruthy();
    expect(completedBadge).toHaveClass('bg-green-500');
  });

  it('shows matched count, left orphans, right orphans cards', () => {
    mockUseQuery.mockReturnValue({ data: mockRunData, isLoading: false, error: null });
    const { container } = render(<RunDetailPage />);

    // Card header titles exist -- use getAllByText for "Left Orphans" / "Right Orphans" which appear as card titles AND tab triggers
    expect(screen.getAllByText(/Matched/).length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText(/Left Orphans/).length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText(/Right Orphans/).length).toBeGreaterThanOrEqual(1);

    // The count values appear as bold text inside the summary cards
    // Use querySelectorAll to find the bold count values
    const boldValues = container.querySelectorAll('.text-2xl.font-bold');
    const values = Array.from(boldValues).map(el => el.textContent);
    expect(values).toContain('900');
    expect(values).toContain('100');
    expect(values).toContain('50');
  });

  it('shows results preview tabs (Matched, Left Orphans, Right Orphans)', () => {
    mockUseQuery.mockReturnValue({ data: mockRunData, isLoading: false, error: null });
    render(<RunDetailPage />);

    expect(screen.getByText('Results Preview')).toBeInTheDocument();
    expect(screen.getByText('Matched (900)')).toBeInTheDocument();
    expect(screen.getByText('Left Orphans (100)')).toBeInTheDocument();
    expect(screen.getByText('Right Orphans (50)')).toBeInTheDocument();
  });

  it('shows "Back to Runs" link in error state', () => {
    mockUseQuery.mockReturnValue({ data: undefined, isLoading: false, error: new Error('fail') });
    render(<RunDetailPage />);

    const backLink = screen.getByText('Back to Runs').closest('a');
    expect(backLink).toHaveAttribute('href', '/runs');
  });

  it('shows export section', () => {
    mockUseQuery.mockReturnValue({ data: mockRunData, isLoading: false, error: null });
    render(<RunDetailPage />);

    expect(screen.getByText('Export Results')).toBeInTheDocument();
    expect(screen.getByText(/Download Matched Records/)).toBeInTheDocument();
    expect(screen.getByText(/Download Left Orphans/)).toBeInTheDocument();
    expect(screen.getByText(/Download Right Orphans/)).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// 5. Recipes Page
// ---------------------------------------------------------------------------
describe('Recipes Page', () => {
  it('renders "Recipes" heading', () => {
    render(<RecipesPage />);
    expect(screen.getByText('Recipes')).toBeInTheDocument();
  });

  it('shows "Create Recipe" button/link', () => {
    render(<RecipesPage />);
    const link = screen.getByText('Create Recipe').closest('a');
    expect(link).toHaveAttribute('href', '/reconcile');
  });

  it('shows sample recipe in list', () => {
    render(<RecipesPage />);
    expect(screen.getByText('invoice-payment-match')).toBeInTheDocument();
  });

  it('clicking recipe selects it and shows details', () => {
    render(<RecipesPage />);

    // Click on the sample recipe
    fireEvent.click(screen.getByText('invoice-payment-match'));

    // Should show match rules heading
    expect(screen.getByText('Match Rules')).toBeInTheDocument();
    // Should show rule name
    expect(screen.getByText('id_and_amount_match')).toBeInTheDocument();
  });

  it('shows match rules in recipe detail', () => {
    render(<RecipesPage />);
    fireEvent.click(screen.getByText('invoice-payment-match'));

    expect(screen.getByText('id_and_amount_match')).toBeInTheDocument();
    expect(screen.getByText('1:1')).toBeInTheDocument();
    expect(screen.getByText(/invoice_id eq payment_ref/)).toBeInTheDocument();
    expect(screen.getByText(/amount tolerance paid_amount/)).toBeInTheDocument();
  });

  it('shows JSON textarea', () => {
    render(<RecipesPage />);
    fireEvent.click(screen.getByText('invoice-payment-match'));

    expect(screen.getByText('JSON')).toBeInTheDocument();
    // The textarea should contain JSON representation
    const textarea = screen.getByRole('textbox');
    expect(textarea).toBeInTheDocument();
    expect((textarea as HTMLTextAreaElement).value).toContain('invoice-payment-match');
  });

  it('copy JSON button copies to clipboard', async () => {
    // Spy on the actual navigator.clipboard.writeText
    const writeTextSpy = jest.spyOn(navigator.clipboard, 'writeText').mockResolvedValue(undefined);

    render(<RecipesPage />);
    fireEvent.click(screen.getByText('invoice-payment-match'));

    fireEvent.click(screen.getByText('Copy JSON'));

    // Wait for the async clipboard operation and resulting state update
    await waitFor(() => {
      expect(screen.getByText('Copied!')).toBeInTheDocument();
    });

    expect(writeTextSpy).toHaveBeenCalledWith(
      expect.stringContaining('invoice-payment-match')
    );

    writeTextSpy.mockRestore();
  });

  it('validate button triggers validation', async () => {
    mockValidateRecipe.mockResolvedValue({ valid: true, errors: [] });
    render(<RecipesPage />);
    fireEvent.click(screen.getByText('invoice-payment-match'));

    await act(async () => {
      fireEvent.click(screen.getByText('Validate'));
    });

    await waitFor(() => {
      expect(mockValidateRecipe).toHaveBeenCalled();
    });
  });

  it('shows validation result', async () => {
    mockValidateRecipe.mockResolvedValue({ valid: true, errors: [] });
    render(<RecipesPage />);
    fireEvent.click(screen.getByText('invoice-payment-match'));

    await act(async () => {
      fireEvent.click(screen.getByText('Validate'));
    });

    await waitFor(() => {
      expect(screen.getByText('Recipe is valid')).toBeInTheDocument();
    });
  });
});

// ---------------------------------------------------------------------------
// 6. Reconcile Page
// ---------------------------------------------------------------------------
describe('Reconcile Page', () => {
  it('shows "Recipe Builder" heading and "Start Conversation" button initially', () => {
    render(<ReconcilePage />);
    expect(screen.getByText('Recipe Builder')).toBeInTheDocument();
    expect(screen.getByText('Start Conversation')).toBeInTheDocument();
  });

  it('clicking "Start Conversation" sends initial message', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        session_id: 'sess-1',
        phase: 'greeting',
        message: {
          role: 'agent',
          segments: [{ type: 'text', content: 'Hello! What data would you like to reconcile?' }],
          timestamp: new Date().toISOString(),
        },
      }),
    });

    render(<ReconcilePage />);

    await act(async () => {
      fireEvent.click(screen.getByText('Start Conversation'));
    });

    await waitFor(() => {
      expect(mockFetch).toHaveBeenCalledWith('/api/chat', expect.objectContaining({
        method: 'POST',
      }));
    });
  });

  it('shows chat messages after API response', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        session_id: 'sess-1',
        phase: 'greeting',
        message: {
          role: 'agent',
          segments: [{ type: 'text', content: 'Hello! What data would you like to reconcile?' }],
          timestamp: new Date().toISOString(),
        },
      }),
    });

    render(<ReconcilePage />);

    await act(async () => {
      fireEvent.click(screen.getByText('Start Conversation'));
    });

    await waitFor(() => {
      // User message (sent by handleStart)
      expect(screen.getByTestId('user-message')).toBeInTheDocument();
      // Agent response
      expect(screen.getByTestId('agent-message')).toBeInTheDocument();
    });
  });

  it('shows loading spinner while waiting for response', async () => {
    // Make fetch never resolve
    mockFetch.mockReturnValueOnce(new Promise(() => {}));

    render(<ReconcilePage />);

    await act(async () => {
      fireEvent.click(screen.getByText('Start Conversation'));
    });

    expect(screen.getByText('Thinking...')).toBeInTheDocument();
  });

  it('shows reset button that clears state', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        session_id: 'sess-1',
        phase: 'greeting',
        message: {
          role: 'agent',
          segments: [{ type: 'text', content: 'Hello!' }],
          timestamp: new Date().toISOString(),
        },
      }),
    });

    render(<ReconcilePage />);

    await act(async () => {
      fireEvent.click(screen.getByText('Start Conversation'));
    });

    await waitFor(() => {
      expect(screen.getByText('Reset')).toBeInTheDocument();
    });

    await act(async () => {
      fireEvent.click(screen.getByText('Reset'));
    });

    // After reset, should show the start screen again
    expect(screen.getByText('Start Conversation')).toBeInTheDocument();
  });

  it('shows phase indicator', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        session_id: 'sess-1',
        phase: 'greeting',
        message: {
          role: 'agent',
          segments: [{ type: 'text', content: 'Hello!' }],
          timestamp: new Date().toISOString(),
        },
      }),
    });

    render(<ReconcilePage />);

    await act(async () => {
      fireEvent.click(screen.getByText('Start Conversation'));
    });

    await waitFor(() => {
      expect(screen.getByText('greeting')).toBeInTheDocument();
    });
  });

  it('shows input field and send button', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        session_id: 'sess-1',
        phase: 'greeting',
        message: {
          role: 'agent',
          segments: [{ type: 'text', content: 'Hello!' }],
          timestamp: new Date().toISOString(),
        },
      }),
    });

    render(<ReconcilePage />);

    await act(async () => {
      fireEvent.click(screen.getByText('Start Conversation'));
    });

    await waitFor(() => {
      expect(screen.getByPlaceholderText('Type your message...')).toBeInTheDocument();
      expect(screen.getByLabelText('Send')).toBeInTheDocument();
    });
  });

  it('send button disabled when input is empty or loading', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        session_id: 'sess-1',
        phase: 'greeting',
        message: {
          role: 'agent',
          segments: [{ type: 'text', content: 'Hello!' }],
          timestamp: new Date().toISOString(),
        },
      }),
    });

    render(<ReconcilePage />);

    await act(async () => {
      fireEvent.click(screen.getByText('Start Conversation'));
    });

    await waitFor(() => {
      const sendButton = screen.getByLabelText('Send');
      // Empty input => disabled
      expect(sendButton).toBeDisabled();
    });
  });

  it('error handling when API call fails', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      statusText: 'Internal Server Error',
      json: async () => ({ error: 'Something went wrong' }),
    });

    render(<ReconcilePage />);

    await act(async () => {
      fireEvent.click(screen.getByText('Start Conversation'));
    });

    await waitFor(() => {
      // The error message should be displayed in a chat message
      const agentMessages = screen.getAllByTestId('agent-message');
      const errorMsg = agentMessages.find(el => el.textContent?.includes('Error:'));
      expect(errorMsg).toBeTruthy();
    });
  });
});
