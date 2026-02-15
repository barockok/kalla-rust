'use client';

import { useState, useCallback, useRef } from 'react';
import { Upload } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { FileUploadPill } from './FileUploadPill';
import { uploadFile } from '@/lib/upload-client';
import type { UploadProgress } from '@/lib/upload-client';
import type { FileAttachment } from '@/lib/chat-types';
import { cn } from '@/lib/utils';

interface UploadRequestCardProps {
  message: string;
  sessionId: string;
  onFileUploaded: (attachment: FileAttachment) => void;
  disabled?: boolean;
}

export function UploadRequestCard({
  message,
  sessionId,
  onFileUploaded,
  disabled,
}: UploadRequestCardProps) {
  const [pendingFile, setPendingFile] = useState<File | null>(null);
  const [uploadProgress, setUploadProgress] = useState<UploadProgress | null>(null);
  const [fileAttachment, setFileAttachment] = useState<FileAttachment | null>(null);
  const [isDragging, setIsDragging] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const isComplete = fileAttachment !== null;
  const isDisabled = disabled || isComplete;

  const handleFileSelect = useCallback(
    async (file: File) => {
      setPendingFile(file);
      setFileAttachment(null);
      setUploadProgress({ phase: 'presigning', percent: 0 });

      try {
        const attachment = await uploadFile(file, sessionId, setUploadProgress);
        setFileAttachment(attachment);
        onFileUploaded(attachment);
      } catch {
        // Error already set via onProgress callback
      }
    },
    [sessionId, onFileUploaded],
  );

  const handleDragOver = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      if (!isDisabled) setIsDragging(true);
    },
    [isDisabled],
  );

  const handleDragLeave = useCallback(() => {
    setIsDragging(false);
  }, []);

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      setIsDragging(false);
      if (isDisabled) return;
      const file = e.dataTransfer.files[0];
      if (file) handleFileSelect(file);
    },
    [isDisabled, handleFileSelect],
  );

  const handleFileInputChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const file = e.target.files?.[0];
      if (file) handleFileSelect(file);
    },
    [handleFileSelect],
  );

  // When disabled or upload complete, show the uploaded file info
  if (isComplete && fileAttachment) {
    return (
      <div className="rounded-lg border bg-muted/50 p-4 max-w-md">
        <p className="text-sm text-muted-foreground mb-2">{message}</p>
        <FileUploadPill
          filename={fileAttachment.filename}
          progress={{ phase: 'done', percent: 100 }}
          attachment={fileAttachment}
          onRemove={() => {}}
        />
      </div>
    );
  }

  return (
    <div className="rounded-lg border bg-muted/50 p-4 max-w-md">
      <p className="text-sm text-foreground mb-3">{message}</p>

      {/* Show pill during upload */}
      {pendingFile && uploadProgress && (
        <div className="mb-3">
          <FileUploadPill
            filename={pendingFile.name}
            progress={uploadProgress}
            attachment={fileAttachment}
            onRemove={() => {
              setPendingFile(null);
              setUploadProgress(null);
              setFileAttachment(null);
              if (fileInputRef.current) fileInputRef.current.value = '';
            }}
          />
        </div>
      )}

      {/* Drop zone */}
      {!pendingFile && (
        <div
          className={cn(
            'flex flex-col items-center justify-center gap-2 rounded-md border-2 border-dashed p-6 transition-colors',
            isDragging
              ? 'border-primary bg-primary/5'
              : 'border-muted-foreground/25 hover:border-muted-foreground/50',
            isDisabled && 'opacity-50 pointer-events-none',
          )}
          onDragOver={handleDragOver}
          onDragLeave={handleDragLeave}
          onDrop={handleDrop}
        >
          <Upload className="h-6 w-6 text-muted-foreground" />
          <p className="text-xs text-muted-foreground">
            Drag and drop a CSV file here, or
          </p>
          <input
            ref={fileInputRef}
            type="file"
            accept=".csv"
            className="hidden"
            onChange={handleFileInputChange}
          />
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={() => fileInputRef.current?.click()}
            disabled={isDisabled}
          >
            Choose CSV file
          </Button>
        </div>
      )}
    </div>
  );
}
