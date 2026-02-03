'use client';

import type { ChatMessage as ChatMessageType, ChatSegment } from '@/lib/chat-types';
import { MatchProposalCard } from './MatchProposalCard';
import { cn } from '@/lib/utils';

interface ChatMessageProps {
  message: ChatMessageType;
  onCardAction?: (cardId: string, action: string, value?: unknown) => void;
}

export function ChatMessage({ message, onCardAction }: ChatMessageProps) {
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
        {message.segments.map((segment, i) => (
          <SegmentRenderer key={i} segment={segment} isAgent={isAgent} onCardAction={onCardAction} />
        ))}
        <span className="text-xs text-muted-foreground">
          {new Date(message.timestamp).toLocaleTimeString()}
        </span>
      </div>
    </div>
  );
}

function SegmentRenderer({ segment, isAgent, onCardAction }: {
  segment: ChatSegment; isAgent: boolean;
  onCardAction?: (cardId: string, action: string, value?: unknown) => void;
}) {
  if (segment.type === 'text' && segment.content) {
    return (
      <div className={cn(
        'rounded-lg px-4 py-2 text-sm whitespace-pre-wrap',
        isAgent ? 'bg-muted text-foreground' : 'bg-primary text-primary-foreground'
      )}>
        {segment.content}
      </div>
    );
  }
  if (segment.type === 'card' && segment.card_type === 'match_proposal') {
    return <MatchProposalCard cardId={segment.card_id!} data={segment.data!} onAction={onCardAction} />;
  }
  return null;
}
