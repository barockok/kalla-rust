import React from 'react';
import { render, screen, waitFor, fireEvent, act } from '@testing-library/react';

// ── Mocks ────────────────────────────────────────────────────────────────────

const mockPathname = jest.fn().mockReturnValue('/');
jest.mock('next/navigation', () => ({
  usePathname: () => mockPathname(),
}));

jest.mock('next/link', () => {
  return function Link({ href, children, className }: { href: string; children: React.ReactNode; className?: string }) {
    return <a href={href} className={className}>{children}</a>;
  };
});

const mockFetch = jest.fn();
global.fetch = mockFetch;

// ── Imports (after mocks) ────────────────────────────────────────────────────

import { Navigation } from '@/components/navigation';
import { Providers } from '@/components/providers';
import { ResultSummary } from '@/components/ResultSummary';
import { PrimaryKeyConfirmation } from '@/components/PrimaryKeyConfirmation';
import { SourcePreview } from '@/components/SourcePreview';

// ── Shared setup ─────────────────────────────────────────────────────────────

beforeEach(() => {
  jest.clearAllMocks();
  mockPathname.mockReturnValue('/');
});

// ═══════════════════════════════════════════════════════════════════════════════
// Navigation
// ═══════════════════════════════════════════════════════════════════════════════

describe('Navigation', () => {
  it('renders all nav items', () => {
    render(<Navigation />);
    expect(screen.getByText('Dashboard')).toBeInTheDocument();
    expect(screen.getByText('Data Sources')).toBeInTheDocument();
    expect(screen.getByText('New Reconciliation')).toBeInTheDocument();
    expect(screen.getByText('Run History')).toBeInTheDocument();
    expect(screen.getByText('Recipes')).toBeInTheDocument();
  });

  it('shows "Kalla" brand text', () => {
    render(<Navigation />);
    expect(screen.getByText('Kalla')).toBeInTheDocument();
  });

  it('shows "Universal Reconciliation Engine" tagline', () => {
    render(<Navigation />);
    expect(screen.getByText('Universal Reconciliation Engine')).toBeInTheDocument();
  });

  it('active link has text-primary class based on pathname', () => {
    mockPathname.mockReturnValue('/sources');
    render(<Navigation />);

    const dataSourcesLink = screen.getByText('Data Sources').closest('a');
    expect(dataSourcesLink).toHaveClass('text-primary');

    const dashboardLink = screen.getByText('Dashboard').closest('a');
    expect(dashboardLink).toHaveClass('text-muted-foreground');
  });
});

// ═══════════════════════════════════════════════════════════════════════════════
// Providers
// ═══════════════════════════════════════════════════════════════════════════════

describe('Providers', () => {
  it('renders children', () => {
    render(
      <Providers>
        <div>Hello World</div>
      </Providers>,
    );
    expect(screen.getByText('Hello World')).toBeInTheDocument();
  });

  it('children can access react-query context', () => {
    const { useQueryClient } = require('@tanstack/react-query');
    function TestChild() {
      const client = useQueryClient();
      return <div>{client ? 'query-client-available' : 'no-client'}</div>;
    }

    render(
      <Providers>
        <TestChild />
      </Providers>,
    );
    expect(screen.getByText('query-client-available')).toBeInTheDocument();
  });
});

// ═══════════════════════════════════════════════════════════════════════════════
// ResultSummary
// ═══════════════════════════════════════════════════════════════════════════════

describe('ResultSummary', () => {
  it('renders match rate correctly (90/100 = 90.0%)', () => {
    render(
      <ResultSummary
        matchedCount={90}
        unmatchedLeftCount={10}
        unmatchedRightCount={5}
        totalLeftCount={100}
        totalRightCount={95}
      />,
    );
    expect(screen.getByText('90.0%')).toBeInTheDocument();
  });

  it('shows "Excellent" indicator for rate >= 90%', () => {
    render(
      <ResultSummary
        matchedCount={90}
        unmatchedLeftCount={10}
        unmatchedRightCount={5}
        totalLeftCount={100}
        totalRightCount={95}
      />,
    );
    expect(screen.getByText('Excellent')).toBeInTheDocument();
  });

  it('shows "Fair" indicator for rate >= 70% but < 90%', () => {
    render(
      <ResultSummary
        matchedCount={75}
        unmatchedLeftCount={25}
        unmatchedRightCount={10}
        totalLeftCount={100}
        totalRightCount={85}
      />,
    );
    expect(screen.getByText('Fair')).toBeInTheDocument();
  });

  it('shows "Needs Review" for rate < 70%', () => {
    render(
      <ResultSummary
        matchedCount={50}
        unmatchedLeftCount={50}
        unmatchedRightCount={30}
        totalLeftCount={100}
        totalRightCount={80}
      />,
    );
    expect(screen.getByText('Needs Review')).toBeInTheDocument();
  });

  it('shows stat cards with correct counts', () => {
    render(
      <ResultSummary
        matchedCount={90}
        unmatchedLeftCount={10}
        unmatchedRightCount={5}
        totalLeftCount={100}
        totalRightCount={95}
      />,
    );
    expect(screen.getByText('Matched')).toBeInTheDocument();
    expect(screen.getByText('90')).toBeInTheDocument();
    expect(screen.getByText('Left Orphans')).toBeInTheDocument();
    expect(screen.getByText('10')).toBeInTheDocument();
    expect(screen.getByText('Right Orphans')).toBeInTheDocument();
    expect(screen.getByText('5')).toBeInTheDocument();
  });

  it('shows issues list when match rate < 80%', () => {
    render(
      <ResultSummary
        matchedCount={60}
        unmatchedLeftCount={40}
        unmatchedRightCount={20}
        totalLeftCount={100}
        totalRightCount={80}
      />,
    );
    expect(screen.getByTestId('issues-list')).toBeInTheDocument();
    expect(screen.getByText('Low overall match rate')).toBeInTheDocument();
  });

  it('does NOT show issues list when match rate >= 80%', () => {
    render(
      <ResultSummary
        matchedCount={90}
        unmatchedLeftCount={10}
        unmatchedRightCount={5}
        totalLeftCount={100}
        totalRightCount={95}
      />,
    );
    expect(screen.queryByTestId('issues-list')).not.toBeInTheDocument();
  });

  it('handles totalLeftCount = 0 gracefully (no division by zero)', () => {
    render(
      <ResultSummary
        matchedCount={0}
        unmatchedLeftCount={0}
        unmatchedRightCount={0}
        totalLeftCount={0}
        totalRightCount={0}
      />,
    );
    expect(screen.getByText('0.0%')).toBeInTheDocument();
  });
});

// ═══════════════════════════════════════════════════════════════════════════════
// PrimaryKeyConfirmation
// ═══════════════════════════════════════════════════════════════════════════════

describe('PrimaryKeyConfirmation', () => {
  const defaultProps = {
    sourceAlias: 'orders',
    onConfirm: jest.fn(),
    onCancel: jest.fn(),
  };

  function mockPrimaryKeyFetch(data: {
    alias: string;
    detected_keys: string[];
    confidence: string;
  }) {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => data,
    });
  }

  it('shows loading state initially', () => {
    mockFetch.mockReturnValue(new Promise(() => {})); // never resolves
    render(<PrimaryKeyConfirmation {...defaultProps} />);
    expect(screen.getByText(/Detecting primary key for orders/)).toBeInTheDocument();
  });

  it('displays detected keys after fetch', async () => {
    mockPrimaryKeyFetch({
      alias: 'orders',
      detected_keys: ['order_id', 'customer_id'],
      confidence: 'high',
    });

    await act(async () => {
      render(<PrimaryKeyConfirmation {...defaultProps} />);
    });

    await waitFor(() => {
      expect(screen.getByText('order_id')).toBeInTheDocument();
      expect(screen.getByText('customer_id')).toBeInTheDocument();
    });
  });

  it('shows high confidence message when confidence is "high"', async () => {
    mockPrimaryKeyFetch({
      alias: 'orders',
      detected_keys: ['order_id'],
      confidence: 'high',
    });

    await act(async () => {
      render(<PrimaryKeyConfirmation {...defaultProps} />);
    });

    await waitFor(() => {
      expect(
        screen.getByText('Detected primary key with high confidence:'),
      ).toBeInTheDocument();
    });
  });

  it('shows low confidence message when confidence is not "high"', async () => {
    mockPrimaryKeyFetch({
      alias: 'orders',
      detected_keys: ['order_id'],
      confidence: 'low',
    });

    await act(async () => {
      render(<PrimaryKeyConfirmation {...defaultProps} />);
    });

    await waitFor(() => {
      expect(
        screen.getByText('Could not auto-detect primary key. Please specify:'),
      ).toBeInTheDocument();
    });
  });

  it('confirm button calls onConfirm with selected keys', async () => {
    const onConfirm = jest.fn();
    mockPrimaryKeyFetch({
      alias: 'orders',
      detected_keys: ['order_id'],
      confidence: 'high',
    });

    await act(async () => {
      render(
        <PrimaryKeyConfirmation
          {...defaultProps}
          onConfirm={onConfirm}
        />,
      );
    });

    await waitFor(() => {
      expect(screen.getByText('order_id')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText('Confirm'));
    expect(onConfirm).toHaveBeenCalledWith(['order_id']);
  });

  it('cancel button calls onCancel', async () => {
    const onCancel = jest.fn();
    mockPrimaryKeyFetch({
      alias: 'orders',
      detected_keys: ['order_id'],
      confidence: 'high',
    });

    await act(async () => {
      render(
        <PrimaryKeyConfirmation
          {...defaultProps}
          onCancel={onCancel}
        />,
      );
    });

    await waitFor(() => {
      expect(screen.getByText('order_id')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText('Cancel'));
    expect(onCancel).toHaveBeenCalledTimes(1);
  });

  it('custom key input clears selected keys', async () => {
    mockPrimaryKeyFetch({
      alias: 'orders',
      detected_keys: ['order_id'],
      confidence: 'high',
    });

    await act(async () => {
      render(<PrimaryKeyConfirmation {...defaultProps} />);
    });

    await waitFor(() => {
      expect(screen.getByText('order_id')).toBeInTheDocument();
    });

    const checkbox = screen.getByRole('checkbox');
    expect(checkbox).toBeChecked();

    const customInput = screen.getByPlaceholderText('column_name');
    fireEvent.change(customInput, { target: { value: 'my_custom_key' } });

    expect(checkbox).not.toBeChecked();
  });

  it('confirm button disabled when no keys selected and no custom key', async () => {
    mockPrimaryKeyFetch({
      alias: 'orders',
      detected_keys: ['order_id'],
      confidence: 'high',
    });

    await act(async () => {
      render(<PrimaryKeyConfirmation {...defaultProps} />);
    });

    await waitFor(() => {
      expect(screen.getByText('order_id')).toBeInTheDocument();
    });

    // Uncheck the pre-selected key
    const checkbox = screen.getByRole('checkbox');
    fireEvent.click(checkbox);

    expect(screen.getByText('Confirm')).toBeDisabled();
  });
});

// ═══════════════════════════════════════════════════════════════════════════════
// SourcePreview
// ═══════════════════════════════════════════════════════════════════════════════

describe('SourcePreview', () => {
  const samplePreview = {
    alias: 'orders',
    columns: [
      { name: 'id', data_type: 'integer', nullable: false },
      { name: 'name', data_type: 'text', nullable: true },
      { name: 'amount', data_type: 'numeric', nullable: true },
    ],
    rows: [
      ['1', 'Alice', '100.50'],
      ['2', 'null', '200.00'],
    ],
    total_rows: 500,
    preview_rows: 2,
  };

  function mockPreviewFetch(data: typeof samplePreview) {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => data,
    });
  }

  it('shows loading state initially', () => {
    mockFetch.mockReturnValue(new Promise(() => {})); // never resolves
    render(<SourcePreview sourceAlias="orders" />);
    expect(screen.getByText(/Loading preview for orders/)).toBeInTheDocument();
  });

  it('shows preview data in table after fetch', async () => {
    mockPreviewFetch(samplePreview);

    await act(async () => {
      render(<SourcePreview sourceAlias="orders" />);
    });

    await waitFor(() => {
      expect(screen.getByText('orders')).toBeInTheDocument();
    });
  });

  it('shows column names and types', async () => {
    mockPreviewFetch(samplePreview);

    await act(async () => {
      render(<SourcePreview sourceAlias="orders" />);
    });

    await waitFor(() => {
      expect(screen.getByText('id')).toBeInTheDocument();
      expect(screen.getByText('integer')).toBeInTheDocument();
      expect(screen.getByText('name')).toBeInTheDocument();
      expect(screen.getByText('text')).toBeInTheDocument();
      expect(screen.getByText('amount')).toBeInTheDocument();
      expect(screen.getByText('numeric')).toBeInTheDocument();
    });
  });

  it('shows data rows', async () => {
    mockPreviewFetch(samplePreview);

    await act(async () => {
      render(<SourcePreview sourceAlias="orders" />);
    });

    await waitFor(() => {
      expect(screen.getByText('Alice')).toBeInTheDocument();
      expect(screen.getByText('100.50')).toBeInTheDocument();
      expect(screen.getByText('200.00')).toBeInTheDocument();
    });
  });

  it('shows null values in italic', async () => {
    mockPreviewFetch(samplePreview);

    await act(async () => {
      render(<SourcePreview sourceAlias="orders" />);
    });

    await waitFor(() => {
      const nullSpan = screen.getByText('null');
      expect(nullSpan.tagName).toBe('SPAN');
      expect(nullSpan).toHaveClass('italic');
    });
  });

  it('shows error message on fetch failure', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      text: async () => 'Source not found',
    });

    await act(async () => {
      render(<SourcePreview sourceAlias="missing" />);
    });

    await waitFor(() => {
      expect(screen.getByText(/Error: Source not found/)).toBeInTheDocument();
    });
  });

  it('shows row count text', async () => {
    mockPreviewFetch(samplePreview);

    await act(async () => {
      render(<SourcePreview sourceAlias="orders" />);
    });

    await waitFor(() => {
      expect(screen.getByText(/Showing 2 of 500 rows/)).toBeInTheDocument();
    });
  });
});
