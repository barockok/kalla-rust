'use client';

import { useState, useEffect } from 'react';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { Badge } from '@/components/ui/badge';
import { Loader2 } from 'lucide-react';

const API_BASE = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:3001';

interface PreviewResponse {
  alias: string;
  columns: Array<{ name: string; data_type: string; nullable: boolean }>;
  rows: string[][];
  total_rows: number;
  preview_rows: number;
}

interface FieldPreviewProps {
  sourceAlias: string;
}

export function FieldPreview({ sourceAlias }: FieldPreviewProps) {
  const [preview, setPreview] = useState<PreviewResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function fetchPreview() {
      setLoading(true);
      setError(null);
      try {
        const res = await fetch(
          `${API_BASE}/api/sources/${sourceAlias}/preview?limit=3`
        );
        if (!res.ok) {
          throw new Error(await res.text());
        }
        const data: PreviewResponse = await res.json();
        setPreview(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load field preview');
      } finally {
        setLoading(false);
      }
    }
    fetchPreview();
  }, [sourceAlias]);

  if (loading) {
    return (
      <div data-testid="field-preview" className="flex items-center gap-2 p-4 text-muted-foreground">
        <Loader2 className="h-4 w-4 animate-spin" />
        Loading fields for {sourceAlias}...
      </div>
    );
  }

  if (error) {
    return (
      <div data-testid="field-preview" className="p-4 text-red-600">
        Error: {error}
      </div>
    );
  }

  if (!preview) {
    return null;
  }

  function getSampleValues(colIndex: number): string[] {
    return (preview?.rows ?? [])
      .map((row) => row[colIndex])
      .filter((val) => val !== undefined);
  }

  return (
    <div data-testid="field-preview" className="border rounded-lg overflow-hidden">
      <div className="bg-muted px-4 py-2 flex items-center justify-between">
        <h3 className="font-semibold text-sm">Fields for {preview.alias}</h3>
        <span data-testid="field-count" className="text-xs text-muted-foreground">
          {preview.columns.length} column{preview.columns.length !== 1 ? 's' : ''}
        </span>
      </div>

      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Column Name</TableHead>
            <TableHead>Type</TableHead>
            <TableHead>Sample Values</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {preview.columns.map((col, colIdx) => (
            <TableRow key={col.name}>
              <TableCell className="font-medium">
                {col.name}
                {col.nullable && (
                  <Badge variant="secondary" className="ml-2 text-[10px] px-1 py-0">
                    nullable
                  </Badge>
                )}
              </TableCell>
              <TableCell className="text-muted-foreground font-mono text-xs">
                {col.data_type}
              </TableCell>
              <TableCell className="font-mono text-xs">
                {getSampleValues(colIdx).map((val, i) => (
                  <span key={i}>
                    {i > 0 && <span className="text-muted-foreground">, </span>}
                    {val === 'null' ? (
                      <span className="text-muted-foreground italic">null</span>
                    ) : (
                      val
                    )}
                  </span>
                ))}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}
