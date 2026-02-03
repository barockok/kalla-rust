'use client';

import { useState } from 'react';
import { Card, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { ChevronDown, ChevronUp, FileText } from 'lucide-react';

interface RecipeCardProps {
  recipe: Record<string, unknown> | null;
}

export function RecipeCard({ recipe }: RecipeCardProps) {
  const [expanded, setExpanded] = useState(false);
  const [showJson, setShowJson] = useState(false);

  if (!recipe) {
    return (
      <div className="fixed bottom-0 left-0 right-0 border-t bg-background/95 backdrop-blur px-4 py-2">
        <div className="container mx-auto flex items-center gap-2 text-sm text-muted-foreground">
          <FileText className="h-4 w-4" />
          Recipe: No rules defined yet
        </div>
      </div>
    );
  }

  const matchRules = (recipe.match_rules as Array<Record<string, unknown>>) || [];
  const sources = recipe.sources as Record<string, Record<string, unknown>> | undefined;

  return (
    <div className="fixed bottom-0 left-0 right-0 border-t bg-background/95 backdrop-blur">
      <div className="container mx-auto">
        <button
          onClick={() => setExpanded(!expanded)}
          className="w-full flex items-center justify-between px-4 py-2 text-sm hover:bg-muted/50 transition-colors"
        >
          <div className="flex items-center gap-2">
            <FileText className="h-4 w-4" />
            <span className="font-medium">
              Recipe: {matchRules.length} rule{matchRules.length !== 1 ? 's' : ''} defined
            </span>
            {sources && (
              <span className="text-muted-foreground">
                ({String((sources.left as Record<string, unknown>)?.alias ?? '')} ↔ {String((sources.right as Record<string, unknown>)?.alias ?? '')})
              </span>
            )}
          </div>
          {expanded ? <ChevronDown className="h-4 w-4" /> : <ChevronUp className="h-4 w-4" />}
        </button>
        {expanded && (
          <Card className="mx-4 mb-4 border-t-0 rounded-t-none">
            <CardContent className="pt-4 space-y-3">
              {matchRules.map((rule, i) => {
                const conditions = (rule.conditions as Array<Record<string, unknown>>) || [];
                return (
                  <div key={i} className="p-3 rounded-lg border text-sm">
                    <div className="flex items-center justify-between mb-1">
                      <span className="font-medium">{rule.name as string}</span>
                      <Badge variant="outline">{rule.pattern as string}</Badge>
                    </div>
                    <ul className="text-muted-foreground space-y-0.5">
                      {conditions.map((c, j) => (
                        <li key={j}>
                          {c.left as string} {c.op as string} {c.right as string}
                          {c.threshold !== undefined && ` (±${c.threshold})`}
                        </li>
                      ))}
                    </ul>
                  </div>
                );
              })}
              <div className="flex gap-2">
                <Button size="sm" variant="outline" onClick={() => setShowJson(!showJson)}>
                  {showJson ? 'Hide JSON' : 'Show JSON'}
                </Button>
              </div>
              {showJson && (
                <pre className="text-xs bg-muted p-3 rounded-lg overflow-x-auto max-h-60">
                  {JSON.stringify(recipe, null, 2)}
                </pre>
              )}
            </CardContent>
          </Card>
        )}
      </div>
    </div>
  );
}
