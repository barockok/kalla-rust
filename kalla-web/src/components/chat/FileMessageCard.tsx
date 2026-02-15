'use client';

import { File as FileIcon } from 'lucide-react';
import type { FileAttachment } from '@/lib/chat-types';

interface FileMessageCardProps {
  file: FileAttachment;
}

export function FileMessageCard({ file }: FileMessageCardProps) {
  return (
    <div className="inline-flex items-center gap-2 rounded-lg border bg-muted/50 px-3 py-2 text-sm">
      <FileIcon className="h-4 w-4 shrink-0 text-muted-foreground" />
      <span className="font-medium truncate max-w-[200px]">{file.filename}</span>
      <span className="text-xs text-muted-foreground whitespace-nowrap">
        {file.columns.length} columns, {file.row_count} rows
      </span>
    </div>
  );
}
