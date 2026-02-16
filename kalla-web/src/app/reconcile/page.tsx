'use client';

import { useState, useRef, useEffect, useCallback } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Loader2, Send, RotateCcw, Paperclip } from 'lucide-react';
import { ChatMessage } from '@/components/chat/ChatMessage';
import { FileUploadPill } from '@/components/chat/FileUploadPill';
import { uploadFile } from '@/lib/upload-client';
import type { UploadProgress } from '@/lib/upload-client';
import type { ChatMessage as ChatMessageType, CardResponse, FileAttachment } from '@/lib/chat-types';
import { cn } from '@/lib/utils';

export default function ReconcilePage() {
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [messages, setMessages] = useState<ChatMessageType[]>([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [phase, setPhase] = useState('greeting');
  const [started, setStarted] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  // File upload state — supports multiple files (e.g. left + right)
  const [pendingFiles, setPendingFiles] = useState<Array<{
    file: File;
    progress: UploadProgress | null;
    attachment: FileAttachment | null;
  }>>([]);
  const [isDragging, setIsDragging] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  useEffect(() => {
    if (!loading) inputRef.current?.focus();
  }, [loading]);

  const handleFileSelect = useCallback(async (file: File) => {
    // Don't add duplicate filenames
    if (pendingFiles.some(pf => pf.file.name === file.name)) return;

    const idx = pendingFiles.length;
    const newEntry = { file, progress: { phase: 'presigning' as const, percent: 0 }, attachment: null };
    setPendingFiles(prev => [...prev, newEntry]);

    if (!sessionId) {
      setPendingFiles(prev => prev.map((pf, i) =>
        i === idx ? { ...pf, progress: { phase: 'error', percent: 0, error: 'Start a conversation first' } } : pf,
      ));
      return;
    }

    try {
      const attachment = await uploadFile(file, sessionId, (p) => {
        setPendingFiles(prev => prev.map(pf =>
          pf.file.name === file.name ? { ...pf, progress: p } : pf,
        ));
      });
      setPendingFiles(prev => prev.map(pf =>
        pf.file.name === file.name ? { ...pf, attachment } : pf,
      ));
    } catch {
      // Error already set via onProgress callback
    }
  }, [sessionId, pendingFiles]);

  const handleRemoveFile = useCallback((filename?: string) => {
    if (filename) {
      setPendingFiles(prev => prev.filter(pf => pf.file.name !== filename));
    } else {
      setPendingFiles([]);
    }
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  }, []);

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(true);
  }, []);

  const handleDragLeave = useCallback(() => {
    setIsDragging(false);
  }, []);

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
    // Support dropping multiple files
    for (const file of Array.from(e.dataTransfer.files)) {
      handleFileSelect(file);
    }
  }, [handleFileSelect]);

  const handleFileInputChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    for (const file of Array.from(e.target.files || [])) {
      handleFileSelect(file);
    }
  }, [handleFileSelect]);

  const sendMessage = async (text: string, cardResponse?: CardResponse, files?: FileAttachment[]) => {
    if (!text.trim() && !cardResponse) return;
    setLoading(true);

    if (!cardResponse) {
      const userMsg: ChatMessageType = {
        role: 'user',
        segments: [{ type: 'text', content: text }],
        timestamp: new Date().toISOString(),
        files: files || undefined,
      };
      setMessages(prev => [...prev, userMsg]);
      setInput('');
    }

    try {
      const res = await fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          session_id: sessionId,
          message: cardResponse ? undefined : text,
          card_response: cardResponse,
          files: files || undefined,
        }),
      });

      if (!res.ok) {
        const errData = await res.json().catch(() => ({ error: res.statusText }));
        throw new Error(errData.error || 'Request failed');
      }

      const data = await res.json();
      setSessionId(data.session_id);
      setPhase(data.phase);
      if (data.message) setMessages(prev => [...prev, data.message]);
    } catch (err) {
      const errMsg: ChatMessageType = {
        role: 'agent',
        segments: [{ type: 'text', content: `Error: ${err instanceof Error ? err.message : 'Something went wrong'}` }],
        timestamp: new Date().toISOString(),
      };
      setMessages(prev => [...prev, errMsg]);
    } finally {
      setLoading(false);
    }
  };

  const handleCardAction = (cardId: string, action: string, value?: unknown) => {
    sendMessage('', { card_id: cardId, action, value });
  };

  const handleStart = () => {
    setStarted(true);
    sendMessage('Hello, I want to reconcile some data.');
  };

  const handleFileUploaded = (attachment: FileAttachment) => {
    // File uploaded via agent's upload request card - send as a message
    sendMessage(`I've uploaded ${attachment.filename}`, undefined, [attachment]);
  };

  const handleReset = () => {
    setSessionId(null);
    setMessages([]);
    setInput('');
    setPhase('greeting');
    setStarted(false);
    handleRemoveFile(); // clears all
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (input.trim() && !loading) {
      const completedAttachments = pendingFiles
        .filter(pf => pf.attachment !== null)
        .map(pf => pf.attachment!);
      const files = completedAttachments.length > 0 ? completedAttachments : undefined;
      sendMessage(input, undefined, files);
      // Clear file state after sending
      handleRemoveFile();
    }
  };

  if (!started) {
    return (
      <div className="flex flex-col items-center justify-center min-h-[60vh] space-y-6">
        <div className="text-center space-y-2">
          <h1 className="text-3xl font-bold tracking-tight">Recipe Builder</h1>
          <p className="text-muted-foreground max-w-md">
            Build reconciliation recipes by demonstrating matches with examples.
            The AI agent will guide you through the process.
          </p>
        </div>
        <Button size="lg" onClick={handleStart}>
          Start Conversation
        </Button>
      </div>
    );
  }

  return (
    <div
      className={cn(
        'flex flex-col h-[calc(100vh-8rem)]',
        isDragging && 'border-2 border-dashed border-primary',
      )}
      onDragOver={handleDragOver}
      onDragLeave={handleDragLeave}
      onDrop={handleDrop}
    >
      <div className="flex items-center justify-between border-b px-4 py-2">
        <div className="flex items-center gap-2">
          <h1 className="text-lg font-semibold">Recipe Builder</h1>
          <span className="text-xs text-muted-foreground capitalize bg-muted px-2 py-0.5 rounded">
            {phase}
          </span>
        </div>
        <Button variant="ghost" size="sm" onClick={handleReset}>
          <RotateCcw className="h-4 w-4 mr-1" />
          Reset
        </Button>
      </div>
      <div className="flex-1 overflow-y-auto pb-32">
        {messages.map((msg, i) => (
          <ChatMessage
            key={i}
            message={msg}
            sessionId={sessionId || undefined}
            onCardAction={handleCardAction}
            onFileUploaded={handleFileUploaded}
          />
        ))}
        {loading && (
          <div className="flex gap-3 px-4 py-3">
            <div className="h-8 w-8 rounded-full bg-primary text-primary-foreground flex items-center justify-center text-sm font-medium">
              K
            </div>
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" />
              Thinking...
            </div>
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>
      <div className="border-t px-4 py-3 bg-background">
        <div className="max-w-3xl mx-auto">
          {/* File upload pills */}
          {pendingFiles.length > 0 && (
            <div className="mb-2 flex flex-wrap gap-1">
              {pendingFiles.map((pf) => (
                <FileUploadPill
                  key={pf.file.name}
                  filename={pf.file.name}
                  progress={pf.progress}
                  attachment={pf.attachment}
                  onRemove={() => handleRemoveFile(pf.file.name)}
                />
              ))}
            </div>
          )}
          <form onSubmit={handleSubmit} className="flex gap-2">
            {/* Hidden file input — multiple */}
            <input
              ref={fileInputRef}
              type="file"
              accept=".csv"
              multiple
              className="hidden"
              onChange={handleFileInputChange}
            />
            <Input
              ref={inputRef}
              value={input}
              onChange={(e) => setInput(e.target.value)}
              placeholder={loading ? 'Waiting for response...' : 'Type your message...'}
              disabled={loading}
              className="flex-1"
            />
            <Button
              type="button"
              variant="ghost"
              size="icon"
              onClick={() => fileInputRef.current?.click()}
              disabled={loading}
              aria-label="Attach file"
            >
              <Paperclip className="h-4 w-4" />
            </Button>
            <Button type="submit" disabled={loading || !input.trim()} aria-label="Send">
              <Send className="h-4 w-4" />
            </Button>
          </form>
        </div>
      </div>
    </div>
  );
}
