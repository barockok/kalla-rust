"use client";

import { useState, useCallback, useRef } from "react";
import {
  Landmark,
  FileText,
  Upload,
  Loader2,
  CheckCircle2,
  Database,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import type { SourceConfig, SampleData } from "@/lib/wizard-types";
import type { ColumnInfo } from "@/lib/chat-types";

/* ------------------------------------------------------------------ */
/*  Types                                                              */
/* ------------------------------------------------------------------ */

interface Props {
  leftAlias: string;
  rightAlias: string;
  leftLoaded: boolean;
  rightLoaded: boolean;
  onSourceLoaded: (
    side: "left" | "right",
    config: SourceConfig,
    sample: SampleData,
  ) => void;
}

type TabKind = "db" | "csv";

interface LoadScopedResponse {
  alias: string;
  columns: ColumnInfo[];
  rows: string[][];
  total_rows: number;
  preview_rows: number;
}

/* ------------------------------------------------------------------ */
/*  Single source card                                                 */
/* ------------------------------------------------------------------ */

function SourceCard({
  alias,
  side,
  alreadyLoaded,
  onSourceLoaded,
}: {
  alias: string;
  side: "left" | "right";
  alreadyLoaded: boolean;
  onSourceLoaded: Props["onSourceLoaded"];
}) {
  const [activeTab, setActiveTab] = useState<TabKind>("db");
  const [loading, setLoading] = useState(false);
  const [loaded, setLoaded] = useState(alreadyLoaded);
  const [error, setError] = useState<string | null>(null);
  const [csvFileName, setCsvFileName] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  /* ---- DB load ---- */
  const handleDbLoad = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`/api/sources/${encodeURIComponent(alias)}/load-scoped`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ conditions: [], limit: 200 }),
      });
      if (!res.ok) throw new Error(`Load failed (${res.status})`);
      const data: LoadScopedResponse = await res.json();

      const config: SourceConfig = {
        mode: "db",
        loaded: true,
        originalAlias: alias,
        activeAlias: data.alias,
      };
      const sample: SampleData = {
        columns: data.columns,
        rows: data.rows,
        totalRows: data.total_rows,
      };

      setLoaded(true);
      onSourceLoaded(side, config, sample);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Load failed");
    } finally {
      setLoading(false);
    }
  }, [alias, side, onSourceLoaded]);

  /* ---- CSV upload pipeline ---- */
  const handleCsvFile = useCallback(
    async (file: File) => {
      setLoading(true);
      setError(null);
      setCsvFileName(file.name);
      try {
        // 1. Presign
        const presignRes = await fetch("/api/uploads/presign", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ filename: file.name, session_id: "wizard" }),
        });
        if (!presignRes.ok) throw new Error("Presign failed");
        const { upload_url, s3_uri } = await presignRes.json();

        // 2. Upload to S3
        const putRes = await fetch(upload_url, {
          method: "PUT",
          body: file,
        });
        if (!putRes.ok) throw new Error("Upload to S3 failed");

        // 3. Register CSV source
        const registerRes = await fetch("/api/sources/register-csv", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ s3_uri, original_alias: alias }),
        });
        if (!registerRes.ok) throw new Error("CSV registration failed");
        const { alias: csvAlias } = await registerRes.json();

        // 4. Load scoped from the new CSV source
        const loadRes = await fetch(
          `/api/sources/${encodeURIComponent(csvAlias)}/load-scoped`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ conditions: [], limit: 200 }),
          },
        );
        if (!loadRes.ok) throw new Error("CSV load failed");
        const data: LoadScopedResponse = await loadRes.json();

        const config: SourceConfig = {
          mode: "csv",
          loaded: true,
          originalAlias: alias,
          activeAlias: csvAlias,
          csvFileName: file.name,
          csvFileSize: file.size,
          csvRowCount: data.total_rows,
          csvColCount: data.columns.length,
        };
        const sample: SampleData = {
          columns: data.columns,
          rows: data.rows,
          totalRows: data.total_rows,
        };

        setLoaded(true);
        onSourceLoaded(side, config, sample);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Upload failed");
        setCsvFileName(null);
      } finally {
        setLoading(false);
      }
    },
    [alias, side, onSourceLoaded],
  );

  const handleFileChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const file = e.target.files?.[0];
      if (file) handleCsvFile(file);
    },
    [handleCsvFile],
  );

  const handleDrop = useCallback(
    (e: React.DragEvent<HTMLDivElement>) => {
      e.preventDefault();
      const file = e.dataTransfer.files?.[0];
      if (file) handleCsvFile(file);
    },
    [handleCsvFile],
  );

  const handleDragOver = useCallback((e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
  }, []);

  const handleReplace = useCallback(() => {
    setCsvFileName(null);
    setLoaded(false);
    fileInputRef.current?.click();
  }, []);

  /* ---- Tab classes ---- */
  const tabClass = (tab: TabKind) =>
    `flex-1 py-2 text-sm font-medium text-center cursor-pointer transition-colors ${
      activeTab === tab
        ? "border-b-2 border-primary bg-muted/50 text-foreground"
        : "text-muted-foreground hover:text-foreground"
    }`;

  return (
    <div className="flex flex-1 flex-col">
      {/* Header */}
      <div className="flex items-center gap-2 px-4 py-3">
        <Database className="h-4 w-4 text-muted-foreground" />
        <span className="text-sm font-medium">{alias}</span>
        {loaded && (
          <CheckCircle2 className="ml-auto h-4 w-4 text-green-500" />
        )}
      </div>

      {/* Tabs */}
      <div className="flex border-b">
        <button
          type="button"
          className={tabClass("db")}
          onClick={() => setActiveTab("db")}
        >
          Load from Source
        </button>
        <button
          type="button"
          className={tabClass("csv")}
          onClick={() => setActiveTab("csv")}
        >
          Upload CSV
        </button>
      </div>

      {/* Tab body */}
      <div className="flex flex-1 flex-col gap-3 p-4">
        {activeTab === "db" ? (
          /* ---- DB Tab ---- */
          <>
            <p className="text-sm text-muted-foreground">
              Pull a sample of up to 200 rows directly from the connected
              database source.
            </p>

            {loaded ? (
              <div className="flex items-center gap-2 text-sm text-green-600">
                <CheckCircle2 className="h-4 w-4" />
                <span>Loaded</span>
              </div>
            ) : (
              <Button
                variant="outline"
                size="sm"
                onClick={handleDbLoad}
                disabled={loading}
                className="w-fit"
              >
                {loading ? (
                  <>
                    <Loader2 className="h-4 w-4 animate-spin" />
                    Loading...
                  </>
                ) : (
                  <>
                    <Landmark className="h-4 w-4" />
                    Load Sample
                  </>
                )}
              </Button>
            )}

            {!loaded && (
              <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
                <span className="h-2 w-2 rounded-full bg-green-500" />
                Connected
              </div>
            )}
          </>
        ) : (
          /* ---- CSV Tab ---- */
          <>
            <p className="text-sm text-muted-foreground">
              Upload a CSV file to use as the data source for this side.
            </p>

            {csvFileName && loaded ? (
              <div className="flex items-center gap-2 rounded-md bg-green-50 px-3 py-2 text-sm dark:bg-green-950/30">
                <FileText className="h-4 w-4 text-green-600" />
                <span className="flex-1 truncate text-green-700 dark:text-green-400">
                  {csvFileName}
                </span>
                <button
                  type="button"
                  className="text-xs text-muted-foreground hover:text-foreground underline"
                  onClick={handleReplace}
                >
                  Replace file
                </button>
              </div>
            ) : (
              <div
                className="flex flex-col items-center gap-2 rounded-lg border-2 border-dashed border-muted-foreground/25 p-6 text-center transition-colors hover:border-muted-foreground/50"
                onDrop={handleDrop}
                onDragOver={handleDragOver}
              >
                {loading ? (
                  <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
                ) : (
                  <Upload className="h-6 w-6 text-muted-foreground" />
                )}
                <p className="text-sm text-muted-foreground">
                  Drop a CSV file here, or{" "}
                  <button
                    type="button"
                    className="text-primary underline"
                    onClick={() => fileInputRef.current?.click()}
                    disabled={loading}
                  >
                    browse
                  </button>
                </p>
              </div>
            )}

            <input
              ref={fileInputRef}
              type="file"
              accept=".csv"
              className="hidden"
              onChange={handleFileChange}
            />
          </>
        )}

        {error && (
          <p className="text-sm text-destructive">{error}</p>
        )}
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Main component                                                     */
/* ------------------------------------------------------------------ */

export function ExpandedSourceCards({
  leftAlias,
  rightAlias,
  leftLoaded,
  rightLoaded,
  onSourceLoaded,
}: Props) {
  return (
    <div className="flex rounded-xl border-[1.5px] border-border">
      <SourceCard
        alias={leftAlias}
        side="left"
        alreadyLoaded={leftLoaded}
        onSourceLoaded={onSourceLoaded}
      />
      <div className="border-l" />
      <SourceCard
        alias={rightAlias}
        side="right"
        alreadyLoaded={rightLoaded}
        onSourceLoaded={onSourceLoaded}
      />
    </div>
  );
}
