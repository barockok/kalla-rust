"use client";

import { useState, useEffect } from "react";
import { useWizard } from "@/components/wizard/wizard-context";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { ArrowRight, Loader2 } from "lucide-react";
import type { RegisteredSource } from "@/lib/api";
import type { SourcePreview } from "@/lib/chat-types";

export function SelectSources() {
  const { state, dispatch } = useWizard();
  const [sources, setSources] = useState<RegisteredSource[]>([]);
  const [leftAlias, setLeftAlias] = useState("");
  const [rightAlias, setRightAlias] = useState("");
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
      <div className="grid grid-cols-2 gap-8 max-w-2xl">
        <div className="flex flex-col gap-2">
          <label className="text-sm font-medium">Source A (Left)</label>
          <Select
            value={leftAlias}
            onValueChange={setLeftAlias}
            disabled={fetchingList}
          >
            <SelectTrigger>
              <SelectValue
                placeholder={fetchingList ? "Loading..." : "Select source"}
              />
            </SelectTrigger>
            <SelectContent>
              {sources.map((s) => (
                <SelectItem key={s.alias} value={s.alias}>
                  {s.alias}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div className="flex flex-col gap-2">
          <label className="text-sm font-medium">Source B (Right)</label>
          <Select
            value={rightAlias}
            onValueChange={setRightAlias}
            disabled={fetchingList}
          >
            <SelectTrigger>
              <SelectValue
                placeholder={fetchingList ? "Loading..." : "Select source"}
              />
            </SelectTrigger>
            <SelectContent>
              {sources.map((s) => (
                <SelectItem key={s.alias} value={s.alias}>
                  {s.alias}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>

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
