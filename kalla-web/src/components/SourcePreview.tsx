'use client';

import { useState, useEffect } from 'react';

interface ColumnInfo {
  name: string;
  data_type: string;
  nullable: boolean;
}

interface SourcePreviewResponse {
  alias: string;
  columns: ColumnInfo[];
  rows: string[][];
  total_rows: number;
  preview_rows: number;
}

interface SourcePreviewProps {
  sourceAlias: string;
  limit?: number;
}

export function SourcePreview({ sourceAlias, limit = 10 }: SourcePreviewProps) {
  const [preview, setPreview] = useState<SourcePreviewResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function fetchPreview() {
      setLoading(true);
      setError(null);
      try {
        const res = await fetch(
          `/api/sources/${sourceAlias}/preview?limit=${limit}`
        );
        if (!res.ok) {
          throw new Error(await res.text());
        }
        const data: SourcePreviewResponse = await res.json();
        setPreview(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load preview');
      } finally {
        setLoading(false);
      }
    }
    fetchPreview();
  }, [sourceAlias, limit]);

  if (loading) {
    return (
      <div className="p-4 text-gray-500">Loading preview for {sourceAlias}...</div>
    );
  }

  if (error) {
    return (
      <div className="p-4 text-red-600">Error: {error}</div>
    );
  }

  if (!preview) {
    return null;
  }

  return (
    <div className="border rounded-lg overflow-hidden">
      <div className="bg-gray-100 px-4 py-2 flex justify-between items-center">
        <h3 className="font-semibold">{preview.alias}</h3>
        <span className="text-sm text-gray-600">
          Showing {preview.preview_rows} of {preview.total_rows.toLocaleString()} rows
        </span>
      </div>

      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              {preview.columns.map((col) => (
                <th
                  key={col.name}
                  className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                  title={`Type: ${col.data_type}${col.nullable ? ' (nullable)' : ''}`}
                >
                  {col.name}
                  <span className="block font-normal normal-case text-gray-400">
                    {col.data_type}
                  </span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {preview.rows.map((row, rowIdx) => (
              <tr key={rowIdx} className="hover:bg-gray-50">
                {row.map((cell, cellIdx) => (
                  <td
                    key={cellIdx}
                    className="px-4 py-2 text-sm text-gray-900 font-mono whitespace-nowrap"
                  >
                    {cell === 'null' ? (
                      <span className="text-gray-400 italic">null</span>
                    ) : (
                      cell
                    )}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
