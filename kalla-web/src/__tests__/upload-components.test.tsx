import React from 'react';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import { FileUploadPill } from '@/components/chat/FileUploadPill';
import { FileMessageCard } from '@/components/chat/FileMessageCard';
import { UploadRequestCard } from '@/components/chat/UploadRequestCard';
import type { FileAttachment } from '@/lib/chat-types';
import type { UploadProgress } from '@/lib/upload-client';

// Mock upload-client
jest.mock('@/lib/upload-client', () => ({
  uploadFile: jest.fn(),
}));
import { uploadFile } from '@/lib/upload-client';
const mockUploadFile = uploadFile as jest.MockedFunction<typeof uploadFile>;

// ---------------------------------------------------------------------------
// FileUploadPill
// ---------------------------------------------------------------------------
describe('FileUploadPill', () => {
  const onRemove = jest.fn();

  beforeEach(() => {
    onRemove.mockReset();
  });

  test('renders filename (truncated if > 24 chars)', () => {
    render(
      <FileUploadPill
        filename="very-long-filename-that-exceeds-limit.csv"
        progress={null}
        attachment={null}
        onRemove={onRemove}
      />,
    );
    // slice(0, 21) + '...' = "very-long-filename-th..."
    expect(screen.getByText('very-long-filename-th...')).toBeInTheDocument();
  });

  test('renders full filename if <= 24 chars', () => {
    render(
      <FileUploadPill
        filename="short.csv"
        progress={null}
        attachment={null}
        onRemove={onRemove}
      />,
    );
    expect(screen.getByText('short.csv')).toBeInTheDocument();
  });

  test('shows progress bar when uploading', () => {
    const progress: UploadProgress = { phase: 'uploading', percent: 50 };
    const { container } = render(
      <FileUploadPill
        filename="test.csv"
        progress={progress}
        attachment={null}
        onRemove={onRemove}
      />,
    );
    // Progress bar div with style
    const bar = container.querySelector('[style*="width: 50%"]');
    expect(bar).toBeInTheDocument();
  });

  test('shows column/row info when done with attachment', () => {
    const attachment: FileAttachment = {
      upload_id: 'u1',
      filename: 'test.csv',
      s3_uri: 's3://b/k',
      columns: ['a', 'b', 'c'],
      row_count: 42,
    };
    render(
      <FileUploadPill
        filename="test.csv"
        progress={{ phase: 'done', percent: 100 }}
        attachment={attachment}
        onRemove={onRemove}
      />,
    );
    expect(screen.getByText('3 cols, 42 rows')).toBeInTheDocument();
  });

  test('shows error state with error message', () => {
    const progress: UploadProgress = { phase: 'error', percent: 0, error: 'Upload failed' };
    render(
      <FileUploadPill
        filename="test.csv"
        progress={progress}
        attachment={null}
        onRemove={onRemove}
      />,
    );
    expect(screen.getByText('Upload failed')).toBeInTheDocument();
  });

  test('shows "Failed" when error has no message', () => {
    const progress: UploadProgress = { phase: 'error', percent: 0 };
    render(
      <FileUploadPill
        filename="test.csv"
        progress={progress}
        attachment={null}
        onRemove={onRemove}
      />,
    );
    expect(screen.getByText('Failed')).toBeInTheDocument();
  });

  test('calls onRemove when remove button clicked', () => {
    render(
      <FileUploadPill
        filename="test.csv"
        progress={null}
        attachment={null}
        onRemove={onRemove}
      />,
    );
    fireEvent.click(screen.getByLabelText('Remove file'));
    expect(onRemove).toHaveBeenCalledTimes(1);
  });

  test('shows done state when attachment is non-null even without progress', () => {
    const attachment: FileAttachment = {
      upload_id: 'u1',
      filename: 'test.csv',
      s3_uri: 's3://b/k',
      columns: ['x'],
      row_count: 1,
    };
    render(
      <FileUploadPill
        filename="test.csv"
        progress={null}
        attachment={attachment}
        onRemove={onRemove}
      />,
    );
    expect(screen.getByText('1 cols, 1 rows')).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// FileMessageCard
// ---------------------------------------------------------------------------
describe('FileMessageCard', () => {
  test('renders filename and metadata', () => {
    const file: FileAttachment = {
      upload_id: 'u1',
      filename: 'payments.csv',
      s3_uri: 's3://b/k',
      columns: ['id', 'amount', 'date'],
      row_count: 100,
    };
    render(<FileMessageCard file={file} />);
    expect(screen.getByText('payments.csv')).toBeInTheDocument();
    expect(screen.getByText('3 columns, 100 rows')).toBeInTheDocument();
  });

  test('renders with single column', () => {
    const file: FileAttachment = {
      upload_id: 'u2',
      filename: 'simple.csv',
      s3_uri: 's3://b/k2',
      columns: ['val'],
      row_count: 1,
    };
    render(<FileMessageCard file={file} />);
    expect(screen.getByText('simple.csv')).toBeInTheDocument();
    expect(screen.getByText('1 columns, 1 rows')).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// UploadRequestCard
// ---------------------------------------------------------------------------
describe('UploadRequestCard', () => {
  const onFileUploaded = jest.fn();

  beforeEach(() => {
    onFileUploaded.mockReset();
    mockUploadFile.mockReset();
  });

  test('renders message and drop zone', () => {
    render(
      <UploadRequestCard
        message="Please upload your CSV"
        sessionId="sess-1"
        onFileUploaded={onFileUploaded}
      />,
    );
    expect(screen.getByText('Please upload your CSV')).toBeInTheDocument();
    expect(screen.getByText('Choose CSV file')).toBeInTheDocument();
    expect(screen.getByText(/Drag and drop/)).toBeInTheDocument();
  });

  test('successful upload: shows pill, calls onFileUploaded, shows completed state', async () => {
    const attachment: FileAttachment = {
      upload_id: 'u1',
      filename: 'data.csv',
      s3_uri: 's3://b/k',
      columns: ['a', 'b'],
      row_count: 10,
    };

    mockUploadFile.mockImplementation(async (_file, _sessionId, onProgress) => {
      onProgress({ phase: 'presigning', percent: 10 });
      onProgress({ phase: 'uploading', percent: 50 });
      onProgress({ phase: 'done', percent: 100 });
      return attachment;
    });

    render(
      <UploadRequestCard
        message="Upload a file"
        sessionId="sess-1"
        onFileUploaded={onFileUploaded}
      />,
    );

    // Simulate file selection via hidden input
    const input = document.querySelector('input[type="file"]') as HTMLInputElement;
    const file = new File(['a,b\n1,2'], 'data.csv', { type: 'text/csv' });
    await act(async () => {
      fireEvent.change(input, { target: { files: [file] } });
    });

    await waitFor(() => {
      expect(onFileUploaded).toHaveBeenCalledWith(attachment);
    });

    // Should show completed state with filename
    expect(screen.getByText('data.csv')).toBeInTheDocument();
  });

  test('upload error: shows pill with error state', async () => {
    mockUploadFile.mockImplementation(async (_file, _sessionId, onProgress) => {
      onProgress({ phase: 'presigning', percent: 10 });
      onProgress({ phase: 'error', percent: 0, error: 'Network error' });
      throw new Error('Network error');
    });

    render(
      <UploadRequestCard
        message="Upload a file"
        sessionId="sess-1"
        onFileUploaded={onFileUploaded}
      />,
    );

    const input = document.querySelector('input[type="file"]') as HTMLInputElement;
    const file = new File(['a\n1'], 'fail.csv', { type: 'text/csv' });
    await act(async () => {
      fireEvent.change(input, { target: { files: [file] } });
    });

    await waitFor(() => {
      expect(screen.getByText('Network error')).toBeInTheDocument();
    });

    expect(onFileUploaded).not.toHaveBeenCalled();
  });

  test('drag over highlights drop zone', () => {
    const { container } = render(
      <UploadRequestCard
        message="Drop here"
        sessionId="sess-1"
        onFileUploaded={onFileUploaded}
      />,
    );

    const dropZone = container.querySelector('.border-dashed')!;
    fireEvent.dragOver(dropZone, { preventDefault: jest.fn() });
    // Drag leave resets
    fireEvent.dragLeave(dropZone);
  });

  test('drop triggers file upload', async () => {
    const attachment: FileAttachment = {
      upload_id: 'u1',
      filename: 'dropped.csv',
      s3_uri: 's3://b/k',
      columns: ['x'],
      row_count: 1,
    };

    mockUploadFile.mockResolvedValue(attachment);

    render(
      <UploadRequestCard
        message="Drop here"
        sessionId="sess-1"
        onFileUploaded={onFileUploaded}
      />,
    );

    const dropZone = document.querySelector('.border-dashed')!;
    const file = new File(['x\n1'], 'dropped.csv', { type: 'text/csv' });

    await act(async () => {
      fireEvent.drop(dropZone, {
        preventDefault: jest.fn(),
        dataTransfer: { files: [file] },
      });
    });

    await waitFor(() => {
      expect(mockUploadFile).toHaveBeenCalled();
    });
  });

  test('disabled state prevents interaction', () => {
    render(
      <UploadRequestCard
        message="Upload"
        sessionId="sess-1"
        onFileUploaded={onFileUploaded}
        disabled
      />,
    );

    const button = screen.getByText('Choose CSV file');
    expect(button).toBeDisabled();
  });
});
