"use client";

import { useState, useEffect, useMemo } from "react";
import { useWizard } from "@/components/wizard/wizard-context";
import { Button } from "@/components/ui/button";
import {
  ArrowRight,
  Loader2,
  Search,
  CircleCheck,
  Landmark,
  CreditCard,
  Database,
  Building2,
  FileText,
  Upload,
  Wallet,
  Server,
} from "lucide-react";
import type { RegisteredSource } from "@/lib/api";
import type { SourcePreview } from "@/lib/chat-types";

/** Map source_type to a lucide icon. */
function sourceIcon(sourceType: string) {
  switch (sourceType) {
    case "postgres":
      return Database;
    case "csv":
    case "csv_upload":
      return FileText;
    case "upload":
      return Upload;
    case "bank":
      return Landmark;
    case "payment":
    case "stripe":
      return CreditCard;
    case "erp":
    case "accounting":
      return Building2;
    case "expense":
      return Wallet;
    default:
      return Server;
  }
}

/** Derive a human-friendly description from source metadata. */
function sourceDescription(source: RegisteredSource): string {
  const parts = source.uri.split("://");
  const scheme = parts[0] ?? source.source_type;
  return `${scheme} connector`;
}

function SourceCard({
  source,
  selected,
  onSelect,
}: {
  source: RegisteredSource;
  selected: boolean;
  onSelect: () => void;
}) {
  const Icon = sourceIcon(source.source_type);
  return (
    <button
      type="button"
      onClick={onSelect}
      className={`flex w-full items-center gap-3 rounded-lg p-4 text-left transition-colors ${
        selected
          ? "border-2 border-foreground"
          : "border-[1.5px] border-border hover:border-muted-foreground/40"
      }`}
    >
      <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-lg bg-muted">
        <Icon
          className={`h-5 w-5 ${selected ? "text-foreground" : "text-muted-foreground"}`}
        />
      </div>
      <div className="flex min-w-0 flex-1 flex-col gap-0.5">
        <span className="text-sm font-medium text-foreground">
          {source.alias}
        </span>
        <span className="text-xs text-muted-foreground">
          {sourceDescription(source)}
        </span>
      </div>
      {selected && <CircleCheck className="h-5 w-5 shrink-0 text-foreground" />}
    </button>
  );
}

function SourceColumn({
  label,
  sources,
  selected,
  onSelect,
  search,
  onSearchChange,
}: {
  label: string;
  sources: RegisteredSource[];
  selected: string;
  onSelect: (alias: string) => void;
  search: string;
  onSearchChange: (v: string) => void;
}) {
  const filtered = useMemo(
    () =>
      sources.filter((s) =>
        s.alias.toLowerCase().includes(search.toLowerCase()),
      ),
    [sources, search],
  );

  return (
    <div className="flex flex-col gap-3">
      <span className="text-sm font-medium text-foreground">{label}</span>
      <div className="flex items-center gap-2.5 rounded-lg border-[1.5px] border-input px-3 py-2.5">
        <Search className="h-4 w-4 text-muted-foreground" />
        <input
          type="text"
          value={search}
          onChange={(e) => onSearchChange(e.target.value)}
          placeholder="Search sources..."
          className="flex-1 bg-transparent text-[13px] text-foreground placeholder:text-muted-foreground focus:outline-none"
        />
      </div>
      <div className="flex flex-col gap-2">
        {filtered.map((s) => (
          <SourceCard
            key={s.alias}
            source={s}
            selected={selected === s.alias}
            onSelect={() => onSelect(s.alias)}
          />
        ))}
        {filtered.length === 0 && (
          <p className="py-4 text-center text-sm text-muted-foreground">
            No sources found.
          </p>
        )}
      </div>
    </div>
  );
}

export function SelectSources() {
  const { state, dispatch } = useWizard();
  const [sources, setSources] = useState<RegisteredSource[]>([]);
  const [leftAlias, setLeftAlias] = useState("");
  const [rightAlias, setRightAlias] = useState("");
  const [searchA, setSearchA] = useState("");
  const [searchB, setSearchB] = useState("");
  const [loading, setLoading] = useState(false);
  const [fetchingList, setFetchingList] = useState(true);

  useEffect(() => {
    fetch("/api/sources")
      .then((r) => r.json())
      .then((data) => setSources(data))
      .catch(() => setSources([]))
      .finally(() => setFetchingList(false));
  }, []);

  const canContinue = leftAlias && rightAlias && leftAlias !== rightAlias;

  async function handleContinue() {
    if (!canContinue) return;
    setLoading(true);

    try {
      const [previewA, previewB] = await Promise.all([
        fetch(`/api/sources/${leftAlias}/preview?limit=5`).then((r) =>
          r.json(),
        ) as Promise<SourcePreview>,
        fetch(`/api/sources/${rightAlias}/preview?limit=5`).then((r) =>
          r.json(),
        ) as Promise<SourcePreview>,
      ]);

      const leftSrc = sources.find((s) => s.alias === leftAlias)!;
      const rightSrc = sources.find((s) => s.alias === rightAlias)!;

      dispatch({
        type: "SET_SOURCES",
        left: {
          alias: leftSrc.alias,
          uri: leftSrc.uri,
          source_type: leftSrc.source_type,
        },
        right: {
          alias: rightSrc.alias,
          uri: rightSrc.uri,
          source_type: rightSrc.source_type,
        },
      });

      dispatch({
        type: "SET_SCHEMAS",
        schemaLeft: previewA.columns,
        schemaRight: previewB.columns,
        previewLeft: previewA.rows,
        previewRight: previewB.rows,
      });

      dispatch({ type: "SET_STEP", step: 2 });
    } catch (err) {
      dispatch({
        type: "SET_ERROR",
        key: "loadSchemas",
        error:
          err instanceof Error
            ? err.message
            : "Failed to load source schemas",
      });
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="flex flex-col gap-8">
      <div className="flex flex-col gap-1">
        <h1 className="text-2xl font-semibold tracking-tight">
          Select Data Sources
        </h1>
        <p className="text-sm text-muted-foreground">
          Choose two data sources to reconcile against each other. Sources can be
          predefined connectors or CSV uploads.
        </p>
      </div>

      {fetchingList ? (
        <div className="flex items-center gap-2 py-12 justify-center text-muted-foreground">
          <Loader2 className="h-4 w-4 animate-spin" />
          <span className="text-sm">Loading sources...</span>
        </div>
      ) : (
        <div className="grid grid-cols-2 gap-8">
          <SourceColumn
            label="Source A"
            sources={sources}
            selected={leftAlias}
            onSelect={setLeftAlias}
            search={searchA}
            onSearchChange={setSearchA}
          />
          <SourceColumn
            label="Source B"
            sources={sources}
            selected={rightAlias}
            onSelect={setRightAlias}
            search={searchB}
            onSearchChange={setSearchB}
          />
        </div>
      )}

      {leftAlias === rightAlias && leftAlias && (
        <p className="text-sm text-destructive">
          Source A and Source B must be different.
        </p>
      )}

      {state.errors.loadSchemas && (
        <p className="text-sm text-destructive">{state.errors.loadSchemas}</p>
      )}

      <div className="mt-4 flex justify-end border-t pt-6">
        <Button onClick={handleContinue} disabled={!canContinue || loading}>
          {loading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
          Continue
          <ArrowRight className="ml-2 h-4 w-4" />
        </Button>
      </div>
    </div>
  );
}
