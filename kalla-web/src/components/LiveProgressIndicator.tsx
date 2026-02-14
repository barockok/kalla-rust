'use client';

import { useState, useEffect, useRef } from 'react';
import { getRun, RunMetadata } from '@/lib/api';
import { Badge } from '@/components/ui/badge';
import { Loader2, CheckCircle, XCircle } from 'lucide-react';

interface LiveProgressIndicatorProps {
  runId: string;
  onComplete?: (run: RunMetadata) => void;
}

export function LiveProgressIndicator({ runId, onComplete }: LiveProgressIndicatorProps) {
  const [run, setRun] = useState<RunMetadata | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const onCompleteRef = useRef(onComplete);
  onCompleteRef.current = onComplete;

  useEffect(() => {
    let active = true;

    async function fetchRun() {
      try {
        const data = await getRun(runId);
        if (!active) return;
        setRun(data);
        setLoading(false);
        setError(null);

        if (data.status === 'Completed' || data.status === 'Failed') {
          onCompleteRef.current?.(data);
        }
      } catch (err) {
        if (!active) return;
        setError(err instanceof Error ? err.message : 'Failed to fetch run status');
        setLoading(false);
      }
    }

    fetchRun();

    const interval = setInterval(() => {
      // Only poll if we haven't reached a terminal state
      setRun((prev) => {
        if (prev && (prev.status === 'Completed' || prev.status === 'Failed')) {
          clearInterval(interval);
          return prev;
        }
        fetchRun();
        return prev;
      });
    }, 2000);

    return () => {
      active = false;
      clearInterval(interval);
    };
  }, [runId]);

  if (loading) {
    return (
      <div data-testid="live-progress" className="flex items-center gap-3 rounded-lg border p-4">
        <Loader2 data-testid="progress-spinner" className="size-5 animate-spin text-muted-foreground" />
        <span data-testid="progress-status" className="text-sm text-muted-foreground">
          Loading run status...
        </span>
      </div>
    );
  }

  if (error) {
    return (
      <div data-testid="live-progress" className="flex items-center gap-3 rounded-lg border border-red-200 bg-red-50 p-4">
        <XCircle className="size-5 text-red-600" />
        <span data-testid="progress-status" className="text-sm text-red-700">
          {error}
        </span>
      </div>
    );
  }

  if (!run) return null;

  if (run.status === 'Running') {
    return (
      <div data-testid="live-progress" className="flex items-center gap-3 rounded-lg border border-blue-200 bg-blue-50 p-4">
        <Loader2 data-testid="progress-spinner" className="size-5 animate-spin text-blue-600" />
        <div className="flex items-center gap-2">
          <span data-testid="progress-status" className="text-sm font-medium text-blue-700">
            Running reconciliation...
          </span>
          {run.matched_count > 0 && (
            <Badge variant="secondary">{run.matched_count.toLocaleString()} matched</Badge>
          )}
        </div>
      </div>
    );
  }

  if (run.status === 'Completed') {
    return (
      <div data-testid="live-progress" className="flex items-center gap-3 rounded-lg border border-green-200 bg-green-50 p-4">
        <CheckCircle className="size-5 text-green-600" />
        <div className="flex items-center gap-2">
          <span data-testid="progress-status" className="text-sm font-medium text-green-700">
            Reconciliation completed
          </span>
          <Badge variant="secondary">{run.matched_count.toLocaleString()} matched</Badge>
        </div>
      </div>
    );
  }

  // Failed
  return (
    <div data-testid="live-progress" className="flex items-center gap-3 rounded-lg border border-red-200 bg-red-50 p-4">
      <XCircle className="size-5 text-red-600" />
      <span data-testid="progress-status" className="text-sm font-medium text-red-700">
        Reconciliation failed
      </span>
    </div>
  );
}
