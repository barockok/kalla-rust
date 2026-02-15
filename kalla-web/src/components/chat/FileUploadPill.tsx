'use client';

import { File as FileIcon, X, AlertCircle } from 'lucide-react';
import type { UploadProgress } from '@/lib/upload-client';
import type { FileAttachment } from '@/lib/chat-types';

interface FileUploadPillProps {
  filename: string;
  progress: UploadProgress | null;
  attachment: FileAttachment | null;
  onRemove: () => void;
}

export function FileUploadPill({ filename, progress, attachment, onRemove }: FileUploadPillProps) {
  const truncatedName = filename.length > 24 ? filename.slice(0, 21) + '...' : filename;

  const isError = progress?.phase === 'error';
  const isDone = progress?.phase === 'done' || attachment !== null;

  return (
    <div className="inline-flex items-center gap-2 rounded-full border bg-muted px-3 py-1.5 text-sm max-w-xs">
      <FileIcon className="h-4 w-4 shrink-0 text-muted-foreground" />
      <span className="truncate font-medium">{truncatedName}</span>

      {/* Progress bar while uploading */}
      {progress && !isDone && !isError && (
        <div className="w-16 h-1.5 bg-muted-foreground/20 rounded-full overflow-hidden">
          <div
            className="h-full bg-primary rounded-full transition-all duration-300"
            style={{ width: `${progress.percent}%` }}
          />
        </div>
      )}

      {/* Done state: show column/row info */}
      {isDone && attachment && (
        <span className="text-xs text-muted-foreground whitespace-nowrap">
          {attachment.columns.length} cols, {attachment.row_count} rows
        </span>
      )}

      {/* Error state */}
      {isError && (
        <span className="text-xs text-destructive flex items-center gap-1">
          <AlertCircle className="h-3 w-3" />
          {progress?.error || 'Failed'}
        </span>
      )}

      <button
        type="button"
        onClick={onRemove}
        className="shrink-0 rounded-full p-0.5 hover:bg-muted-foreground/20 transition-colors"
        aria-label="Remove file"
      >
        <X className="h-3.5 w-3.5 text-muted-foreground" />
      </button>
    </div>
  );
}
