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
  useParams: () => ({ id: 'test-run-id' }),
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
import { listSources, registerSource } from '@/lib/api';
const mockListSources = listSources as jest.MockedFunction<typeof listSources>;
const mockRegisterSource = registerSource as jest.MockedFunction<typeof registerSource>;

// Mock SourcePreview component (it does internal fetches)
jest.mock('@/components/SourcePreview', () => ({
  SourcePreview: ({ sourceAlias }: { sourceAlias: string }) => (
    <div data-testid="source-preview">Preview for {sourceAlias}</div>
  ),
}));

// Mock PrimaryKeyConfirmation component (it does internal fetches)
jest.mock('@/components/PrimaryKeyConfirmation', () => ({
  PrimaryKeyConfirmation: ({
    sourceAlias,
    onConfirm,
    onCancel,
  }: {
    sourceAlias: string;
    onConfirm: (keys: string[]) => void;
    onCancel: () => void;
  }) => (
    <div data-testid="pk-confirmation">
      PK for {sourceAlias}
      <button onClick={() => onConfirm(['id'])}>Confirm PK</button>
      <button onClick={onCancel}>Cancel PK</button>
    </div>
  ),
}));

// Mock fetch globally
const mockFetch = jest.fn();
global.fetch = mockFetch;

// Import page after mocks are set up
import SourcesPage from '@/app/sources/page';

beforeEach(() => {
  jest.clearAllMocks();
  mockFetch.mockReset();
  mockListSources.mockReset();
  mockRegisterSource.mockReset();
});

// ---------------------------------------------------------------------------
// Helper: render SourcesPage after listSources resolves with given sources
// ---------------------------------------------------------------------------
async function renderWithSources(
  sources: Array<{ alias: string; uri: string; source_type: string; status: string }> = []
) {
  mockListSources.mockResolvedValue(sources);
  await act(async () => {
    render(<SourcesPage />);
  });
  // Wait for loading to finish
  await waitFor(() => {
    expect(screen.queryByText('Loading sources...')).not.toBeInTheDocument();
  });
}

// ---------------------------------------------------------------------------
// 1. listSources rejection => console.error (line 38)
// ---------------------------------------------------------------------------
describe('Sources Page - listSources rejection', () => {
  it('logs console.error when listSources rejects', async () => {
    const consoleSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
    const fetchError = new Error('Network failure');
    mockListSources.mockRejectedValue(fetchError);

    await act(async () => {
      render(<SourcesPage />);
    });

    await waitFor(() => {
      expect(consoleSpy).toHaveBeenCalledWith('Failed to fetch sources:', fetchError);
    });

    // Loading should have finished (isLoading = false via finally block)
    await waitFor(() => {
      expect(screen.queryByText('Loading sources...')).not.toBeInTheDocument();
    });

    consoleSpy.mockRestore();
  });
});

// ---------------------------------------------------------------------------
// 2. handleFileUpload (lines 54-72)
// ---------------------------------------------------------------------------
describe('Sources Page - file upload', () => {
  it('uploads a CSV file and registers it as a source', async () => {
    await renderWithSources([]);

    const fileInput = document.querySelector('input[type="file"]') as HTMLInputElement;
    expect(fileInput).toBeTruthy();

    const csvFile = new File(['col1,col2\na,b'], 'transactions.csv', { type: 'text/csv' });

    await act(async () => {
      fireEvent.change(fileInput, { target: { files: [csvFile] } });
    });

    // The source should appear in the registered sources list
    await waitFor(() => {
      expect(screen.getByText('transactions')).toBeInTheDocument();
      expect(screen.getByText('file://transactions.csv')).toBeInTheDocument();
      expect(screen.getByText('csv')).toBeInTheDocument();
    });

    // Success message should appear
    expect(screen.getByText(/File "transactions.csv" registered as "transactions"/)).toBeInTheDocument();
  });

  it('uploads a Parquet file and sets type to parquet', async () => {
    await renderWithSources([]);

    const fileInput = document.querySelector('input[type="file"]') as HTMLInputElement;
    expect(fileInput).toBeTruthy();

    const parquetFile = new File(['parquet-data'], 'orders.parquet', {
      type: 'application/octet-stream',
    });

    await act(async () => {
      fireEvent.change(fileInput, { target: { files: [parquetFile] } });
    });

    await waitFor(() => {
      expect(screen.getByText('orders')).toBeInTheDocument();
      expect(screen.getByText('file://orders.parquet')).toBeInTheDocument();
      expect(screen.getByText('parquet')).toBeInTheDocument();
    });

    expect(screen.getByText(/File "orders.parquet" registered as "orders"/)).toBeInTheDocument();
  });

  it('does nothing when file input change has no files', async () => {
    await renderWithSources([]);

    const fileInput = document.querySelector('input[type="file"]') as HTMLInputElement;

    await act(async () => {
      fireEvent.change(fileInput, { target: { files: [] } });
    });

    // Should still show "No sources registered yet"
    expect(screen.getByText('No sources registered yet')).toBeInTheDocument();
  });

  it('clears success message after setTimeout', async () => {
    jest.useFakeTimers();
    await renderWithSources([]);

    const fileInput = document.querySelector('input[type="file"]') as HTMLInputElement;
    const csvFile = new File(['data'], 'test.csv', { type: 'text/csv' });

    await act(async () => {
      fireEvent.change(fileInput, { target: { files: [csvFile] } });
    });

    // Success message should be visible
    expect(screen.getByText(/File "test.csv" registered as "test"/)).toBeInTheDocument();

    // Advance timers to clear the success message
    await act(async () => {
      jest.advanceTimersByTime(3000);
    });

    expect(screen.queryByText(/File "test.csv" registered as "test"/)).not.toBeInTheDocument();

    jest.useRealTimers();
  });
});

// ---------------------------------------------------------------------------
// 3. handleAddSource validation - empty alias/URI (lines 77-78)
// ---------------------------------------------------------------------------
describe('Sources Page - empty alias/URI validation', () => {
  it('shows error when alias and URI are both empty', async () => {
    const user = userEvent.setup();
    await renderWithSources([]);

    // Switch to connection string tab
    const connectionTab = screen.getByRole('tab', { name: /Connection String/i });
    await user.click(connectionTab);

    await waitFor(() => {
      expect(screen.getByLabelText('Alias')).toBeInTheDocument();
    });

    // Click Add Source without filling in anything
    await user.click(screen.getByText('Add Source'));

    await waitFor(() => {
      expect(screen.getByText('Please provide both alias and URI')).toBeInTheDocument();
    });

    // registerSource should NOT have been called
    expect(mockRegisterSource).not.toHaveBeenCalled();
  });

  it('shows error when alias is provided but URI is empty', async () => {
    const user = userEvent.setup();
    await renderWithSources([]);

    const connectionTab = screen.getByRole('tab', { name: /Connection String/i });
    await user.click(connectionTab);

    await waitFor(() => {
      expect(screen.getByLabelText('Alias')).toBeInTheDocument();
    });

    await user.type(screen.getByLabelText('Alias'), 'my-source');
    await user.click(screen.getByText('Add Source'));

    await waitFor(() => {
      expect(screen.getByText('Please provide both alias and URI')).toBeInTheDocument();
    });

    expect(mockRegisterSource).not.toHaveBeenCalled();
  });

  it('shows error when URI is provided but alias is empty', async () => {
    const user = userEvent.setup();
    await renderWithSources([]);

    const connectionTab = screen.getByRole('tab', { name: /Connection String/i });
    await user.click(connectionTab);

    await waitFor(() => {
      expect(screen.getByLabelText('URI')).toBeInTheDocument();
    });

    await user.type(screen.getByLabelText('URI'), 'postgres://localhost/db');
    await user.click(screen.getByText('Add Source'));

    await waitFor(() => {
      expect(screen.getByText('Please provide both alias and URI')).toBeInTheDocument();
    });

    expect(mockRegisterSource).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// 4. handleRemoveSource (line 106)
// ---------------------------------------------------------------------------
describe('Sources Page - remove source', () => {
  it('removes a source when the delete button is clicked', async () => {
    await renderWithSources([
      { alias: 'invoices', uri: 'file://invoices.csv', source_type: 'csv', status: 'connected' },
      { alias: 'payments', uri: 'postgres://localhost/db', source_type: 'postgres', status: 'connected' },
    ]);

    // Both sources should be present
    expect(screen.getByText('invoices')).toBeInTheDocument();
    expect(screen.getByText('payments')).toBeInTheDocument();

    // Find all delete (trash) buttons -- they have no accessible text, so get by SVG parent button
    // The delete buttons are the last ghost buttons in each source row
    const allButtons = screen.getAllByRole('button');
    // Filter for buttons that do not have a title attribute (delete buttons have no title)
    const deleteButtons = allButtons.filter(
      (btn) => !btn.getAttribute('title') && btn.closest('.flex.items-center.gap-2')
    );

    // Click the first delete button to remove 'invoices'
    await act(async () => {
      fireEvent.click(deleteButtons[0]);
    });

    // 'invoices' should be removed
    await waitFor(() => {
      expect(screen.queryByText('invoices')).not.toBeInTheDocument();
    });
    // 'payments' should still be there
    expect(screen.getByText('payments')).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// 5. Preview toggle button (lines 236-263)
// ---------------------------------------------------------------------------
describe('Sources Page - preview toggle', () => {
  it('shows SourcePreview when preview button is clicked', async () => {
    await renderWithSources([
      { alias: 'invoices', uri: 'file://invoices.csv', source_type: 'csv', status: 'connected' },
    ]);

    // Click the "Preview data" button
    const previewButton = screen.getByTitle('Preview data');
    await act(async () => {
      fireEvent.click(previewButton);
    });

    await waitFor(() => {
      expect(screen.getByTestId('source-preview')).toBeInTheDocument();
      expect(screen.getByText('Preview for invoices')).toBeInTheDocument();
    });
  });

  it('hides SourcePreview when preview button is clicked again', async () => {
    await renderWithSources([
      { alias: 'invoices', uri: 'file://invoices.csv', source_type: 'csv', status: 'connected' },
    ]);

    const previewButton = screen.getByTitle('Preview data');

    // Click to open
    await act(async () => {
      fireEvent.click(previewButton);
    });

    await waitFor(() => {
      expect(screen.getByTestId('source-preview')).toBeInTheDocument();
    });

    // Click again to close
    await act(async () => {
      fireEvent.click(previewButton);
    });

    await waitFor(() => {
      expect(screen.queryByTestId('source-preview')).not.toBeInTheDocument();
    });
  });
});

// ---------------------------------------------------------------------------
// 6. PK confirm toggle button (lines 265-276)
// ---------------------------------------------------------------------------
describe('Sources Page - PK confirmation toggle', () => {
  it('shows PrimaryKeyConfirmation when PK button is clicked', async () => {
    await renderWithSources([
      { alias: 'invoices', uri: 'file://invoices.csv', source_type: 'csv', status: 'connected' },
    ]);

    const pkButton = screen.getByTitle('Check primary key');
    await act(async () => {
      fireEvent.click(pkButton);
    });

    await waitFor(() => {
      expect(screen.getByTestId('pk-confirmation')).toBeInTheDocument();
      expect(screen.getByText('PK for invoices')).toBeInTheDocument();
    });
  });

  it('hides PrimaryKeyConfirmation when PK button is clicked again', async () => {
    await renderWithSources([
      { alias: 'invoices', uri: 'file://invoices.csv', source_type: 'csv', status: 'connected' },
    ]);

    const pkButton = screen.getByTitle('Check primary key');

    // Click to open
    await act(async () => {
      fireEvent.click(pkButton);
    });

    await waitFor(() => {
      expect(screen.getByTestId('pk-confirmation')).toBeInTheDocument();
    });

    // Click again to close
    await act(async () => {
      fireEvent.click(pkButton);
    });

    await waitFor(() => {
      expect(screen.queryByTestId('pk-confirmation')).not.toBeInTheDocument();
    });
  });
});

// ---------------------------------------------------------------------------
// 7. PK onConfirm callback (line 269-272)
// ---------------------------------------------------------------------------
describe('Sources Page - PK confirm callback', () => {
  it('closes PK panel when Confirm PK is clicked', async () => {
    const consoleSpy = jest.spyOn(console, 'log').mockImplementation(() => {});

    await renderWithSources([
      { alias: 'invoices', uri: 'file://invoices.csv', source_type: 'csv', status: 'connected' },
    ]);

    // Open PK panel
    const pkButton = screen.getByTitle('Check primary key');
    await act(async () => {
      fireEvent.click(pkButton);
    });

    await waitFor(() => {
      expect(screen.getByTestId('pk-confirmation')).toBeInTheDocument();
    });

    // Click Confirm PK
    await act(async () => {
      fireEvent.click(screen.getByText('Confirm PK'));
    });

    // Panel should close (setPkConfirmSource(null))
    await waitFor(() => {
      expect(screen.queryByTestId('pk-confirmation')).not.toBeInTheDocument();
    });

    // Should log the confirmed keys
    expect(consoleSpy).toHaveBeenCalledWith('Confirmed PK for', 'invoices', ':', ['id']);

    consoleSpy.mockRestore();
  });
});

// ---------------------------------------------------------------------------
// 8. PK onCancel callback (line 273)
// ---------------------------------------------------------------------------
describe('Sources Page - PK cancel callback', () => {
  it('closes PK panel when Cancel PK is clicked', async () => {
    await renderWithSources([
      { alias: 'invoices', uri: 'file://invoices.csv', source_type: 'csv', status: 'connected' },
    ]);

    // Open PK panel
    const pkButton = screen.getByTitle('Check primary key');
    await act(async () => {
      fireEvent.click(pkButton);
    });

    await waitFor(() => {
      expect(screen.getByTestId('pk-confirmation')).toBeInTheDocument();
    });

    // Click Cancel PK
    await act(async () => {
      fireEvent.click(screen.getByText('Cancel PK'));
    });

    // Panel should close
    await waitFor(() => {
      expect(screen.queryByTestId('pk-confirmation')).not.toBeInTheDocument();
    });
  });
});
