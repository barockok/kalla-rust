'use client';

import { useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Check, X, HelpCircle } from 'lucide-react';

interface MatchProposalCardProps {
  cardId: string;
  data: Record<string, unknown>;
  onAction?: (cardId: string, action: string, value?: unknown) => void;
}

export function MatchProposalCard({ cardId, data, onAction }: MatchProposalCardProps) {
  const [responded, setResponded] = useState(false);
  const [response, setResponse] = useState<string | null>(null);

  const leftRow = (data.left || {}) as Record<string, string>;
  const rightRow = (data.right || {}) as Record<string, string>;
  const reasoning = data.reasoning as string || '';

  const handleAction = (action: string) => {
    setResponded(true);
    setResponse(action);
    onAction?.(cardId, action, action === 'confirm' ? { left: leftRow, right: rightRow } : undefined);
  };

  return (
    <Card className="w-full max-w-lg">
      <CardHeader className="pb-2">
        <CardTitle className="text-sm flex items-center gap-2">
          Match Proposal
          {responded && (
            <Badge variant={response === 'confirm' ? 'default' : 'destructive'}>
              {response === 'confirm' ? 'Confirmed' : 'Rejected'}
            </Badge>
          )}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="grid grid-cols-2 gap-3 text-xs">
          <div>
            <p className="font-medium text-muted-foreground mb-1">Left Source</p>
            {Object.entries(leftRow).slice(0, 5).map(([k, v]) => (
              <p key={k}><span className="font-medium">{k}:</span> {v}</p>
            ))}
          </div>
          <div>
            <p className="font-medium text-muted-foreground mb-1">Right Source</p>
            {Object.entries(rightRow).slice(0, 5).map(([k, v]) => (
              <p key={k}><span className="font-medium">{k}:</span> {v}</p>
            ))}
          </div>
        </div>
        {reasoning && <p className="text-xs text-muted-foreground italic">{reasoning}</p>}
        {!responded && (
          <div className="flex gap-2">
            <Button size="sm" onClick={() => handleAction('confirm')}>
              <Check className="mr-1 h-3 w-3" /> Yes, match
            </Button>
            <Button size="sm" variant="destructive" onClick={() => handleAction('reject')}>
              <X className="mr-1 h-3 w-3" /> No
            </Button>
            <Button size="sm" variant="outline" onClick={() => handleAction('unsure')}>
              <HelpCircle className="mr-1 h-3 w-3" /> Not sure
            </Button>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
