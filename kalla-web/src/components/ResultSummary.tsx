'use client';

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { CheckCircle, AlertTriangle, XCircle } from 'lucide-react';
import { cn } from '@/lib/utils';

interface ResultSummaryProps {
  matchedCount: number;
  unmatchedLeftCount: number;
  unmatchedRightCount: number;
  totalLeftCount: number;
  totalRightCount: number;
}

function getMatchRate(matched: number, total: number): number {
  if (total === 0) return 0;
  return (matched / total) * 100;
}

function getMatchIndicator(rate: number) {
  if (rate >= 90) {
    return {
      label: 'Excellent',
      icon: CheckCircle,
      colorClass: 'text-green-600',
      badgeClass: 'bg-green-100 text-green-800 border-green-200',
    };
  }
  if (rate >= 70) {
    return {
      label: 'Fair',
      icon: AlertTriangle,
      colorClass: 'text-yellow-600',
      badgeClass: 'bg-yellow-100 text-yellow-800 border-yellow-200',
    };
  }
  return {
    label: 'Needs Review',
    icon: XCircle,
    colorClass: 'text-red-600',
    badgeClass: 'bg-red-100 text-red-800 border-red-200',
  };
}

export function ResultSummary({
  matchedCount,
  unmatchedLeftCount,
  unmatchedRightCount,
  totalLeftCount,
  totalRightCount,
}: ResultSummaryProps) {
  const matchRate = getMatchRate(matchedCount, totalLeftCount);
  const indicator = getMatchIndicator(matchRate);
  const Icon = indicator.icon;

  const issues: string[] = [];
  if (matchRate < 80) {
    issues.push('Low overall match rate');
  }
  if (unmatchedLeftCount > 0 && unmatchedLeftCount / totalLeftCount > 0.2) {
    issues.push('High number of unmatched left records');
  }
  if (unmatchedRightCount > 0 && unmatchedRightCount / totalRightCount > 0.2) {
    issues.push('High number of unmatched right records');
  }

  return (
    <Card data-testid="result-summary">
      <CardHeader>
        <CardTitle>Reconciliation Summary</CardTitle>
      </CardHeader>
      <CardContent className="space-y-6">
        {/* Match rate */}
        <div className="flex items-center gap-4" data-testid="match-rate">
          <span className={cn('text-4xl font-bold', indicator.colorClass)}>
            {matchRate.toFixed(1)}%
          </span>
          <div data-testid="match-indicator">
            <Badge className={indicator.badgeClass}>
              <Icon className="size-3" />
              {indicator.label}
            </Badge>
          </div>
        </div>

        {/* Stat cards */}
        <div className="grid grid-cols-3 gap-4">
          <div className="rounded-lg border p-4 text-center">
            <p className="text-sm text-muted-foreground">Matched</p>
            <p className="text-2xl font-semibold">{matchedCount.toLocaleString()}</p>
          </div>
          <div className="rounded-lg border p-4 text-center">
            <p className="text-sm text-muted-foreground">Left Orphans</p>
            <p className="text-2xl font-semibold">{unmatchedLeftCount.toLocaleString()}</p>
          </div>
          <div className="rounded-lg border p-4 text-center">
            <p className="text-sm text-muted-foreground">Right Orphans</p>
            <p className="text-2xl font-semibold">{unmatchedRightCount.toLocaleString()}</p>
          </div>
        </div>

        {/* Issues */}
        {issues.length > 0 && (
          <div data-testid="issues-list" className="rounded-lg border border-yellow-200 bg-yellow-50 p-4">
            <p className="mb-2 text-sm font-medium text-yellow-800">Potential Issues</p>
            <ul className="space-y-1">
              {issues.map((issue) => (
                <li key={issue} className="flex items-center gap-2 text-sm text-yellow-700">
                  <AlertTriangle className="size-3.5 shrink-0" />
                  {issue}
                </li>
              ))}
            </ul>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
