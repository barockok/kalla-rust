'use client';

import type { ChatMessage as ChatMessageType, ChatSegment, FileAttachment } from '@/lib/chat-types';
import { MatchProposalCard } from './MatchProposalCard';
import { UploadRequestCard } from './UploadRequestCard';
import { FileMessageCard } from './FileMessageCard';
import { MarkdownRenderer } from './MarkdownRenderer';
import { ResultSummary } from '@/components/ResultSummary';
import { LiveProgressIndicator } from '@/components/LiveProgressIndicator';
import { cn } from '@/lib/utils';

interface ChatMessageProps {
  message: ChatMessageType;
  sessionId?: string;
  onCardAction?: (cardId: string, action: string, value?: unknown) => void;
  onFileUploaded?: (attachment: FileAttachment) => void;
}

export function ChatMessage({ message, sessionId, onCardAction, onFileUploaded }: ChatMessageProps) {
  const isAgent = message.role === 'agent';

  return (
    <div
      className={cn('flex gap-3 px-4 py-3', isAgent ? '' : 'flex-row-reverse')}
      data-testid={isAgent ? 'agent-message' : 'user-message'}
    >
      <div className={cn(
        'h-8 w-8 rounded-full flex items-center justify-center text-sm font-medium shrink-0',
        isAgent ? 'bg-primary text-primary-foreground' : 'bg-muted text-muted-foreground'
      )}>
        {isAgent ? 'K' : 'U'}
      </div>
      <div className={cn('flex flex-col gap-2 max-w-[80%]', isAgent ? '' : 'items-end')}>
        {/* Render file attachments */}
        {message.files && message.files.length > 0 && (
          <div className="flex flex-col gap-1">
            {message.files.map((file) => (
              <FileMessageCard key={file.upload_id} file={file} />
            ))}
          </div>
        )}
        {message.segments.map((segment, i) => (
          <SegmentRenderer
            key={i}
            segment={segment}
            isAgent={isAgent}
            sessionId={sessionId}
            onCardAction={onCardAction}
            onFileUploaded={onFileUploaded}
          />
        ))}
        <span className="text-xs text-muted-foreground">
          {new Date(message.timestamp).toLocaleTimeString()}
        </span>
      </div>
    </div>
  );
}

function SegmentRenderer({ segment, isAgent, sessionId, onCardAction, onFileUploaded }: {
  segment: ChatSegment; isAgent: boolean;
  sessionId?: string;
  onCardAction?: (cardId: string, action: string, value?: unknown) => void;
  onFileUploaded?: (attachment: FileAttachment) => void;
}) {
  if (segment.type === 'text' && segment.content) {
    // Only render markdown for agent messages; keep user messages as plain text
    if (isAgent) {
      return (
        <div className="rounded-lg px-4 py-2 bg-muted text-foreground">
          <MarkdownRenderer content={segment.content} />
        </div>
      );
    }
    return (
      <div className={cn(
        'rounded-lg px-4 py-2 text-sm whitespace-pre-wrap',
        'bg-primary text-primary-foreground'
      )}>
        {segment.content}
      </div>
    );
  }
  if (segment.type === 'card' && segment.card_type === 'match_proposal') {
    return <MatchProposalCard cardId={segment.card_id!} data={segment.data!} onAction={onCardAction} />;
  }
  if (segment.type === 'card' && segment.card_type === 'upload_request' && sessionId && onFileUploaded) {
    return (
      <UploadRequestCard
        message={(segment.data?.message as string) || 'Please upload a CSV file.'}
        sessionId={sessionId}
        onFileUploaded={onFileUploaded}
      />
    );
  }
  if (segment.type === 'card' && segment.card_type === 'result_summary' && segment.data) {
    const d = segment.data;
    const matched = Number(d.matched_count ?? 0);
    const unmatchedLeft = Number(d.unmatched_left_count ?? 0);
    const unmatchedRight = Number(d.unmatched_right_count ?? 0);
    return (
      <ResultSummary
        matchedCount={matched}
        unmatchedLeftCount={unmatchedLeft}
        unmatchedRightCount={unmatchedRight}
        totalLeftCount={matched + unmatchedLeft}
        totalRightCount={matched + unmatchedRight}
      />
    );
  }
  if (segment.type === 'card' && segment.card_type === 'progress' && segment.data?.run_id) {
    return <LiveProgressIndicator runId={segment.data.run_id as string} />;
  }
  return null;
}
