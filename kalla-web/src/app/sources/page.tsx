"use client";

import { useState, useEffect } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Database, FileUp, Plus, Trash2, CheckCircle, AlertCircle, Loader2 } from "lucide-react";
import { registerSource, listSources, type RegisteredSource as ApiSource } from "@/lib/api";

interface RegisteredSource {
  alias: string;
  uri: string;
  type: "csv" | "postgres" | "parquet";
  status: "connected" | "error";
}

export default function SourcesPage() {
  const [sources, setSources] = useState<RegisteredSource[]>([]);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    async function fetchSources() {
      try {
        const data = await listSources();
        setSources(data.map((s: ApiSource) => ({
          alias: s.alias,
          uri: s.uri,
          type: s.source_type as "csv" | "postgres" | "parquet",
          status: s.status as "connected" | "error",
        })));
      } catch (err) {
        console.error("Failed to fetch sources:", err);
      } finally {
        setIsLoading(false);
      }
    }
    fetchSources();
  }, []);
  const [alias, setAlias] = useState("");
  const [uri, setUri] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  const handleFileUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    const fileAlias = file.name.replace(/\.[^/.]+$/, "");
    const fileUri = `file://${file.name}`;

    // In a real implementation, we'd upload the file to the server
    // For now, we'll just add it to the local state
    setSources([
      ...sources,
      {
        alias: fileAlias,
        uri: fileUri,
        type: file.name.endsWith(".parquet") ? "parquet" : "csv",
        status: "connected",
      },
    ]);
    setSuccess(`File "${file.name}" registered as "${fileAlias}"`);
    setTimeout(() => setSuccess(null), 3000);
  };

  const handleAddSource = async () => {
    if (!alias || !uri) {
      setError("Please provide both alias and URI");
      return;
    }

    setLoading(true);
    setError(null);

    try {
      await registerSource(alias, uri);

      const type = uri.startsWith("postgres://")
        ? "postgres"
        : uri.endsWith(".parquet")
          ? "parquet"
          : "csv";

      setSources([...sources, { alias, uri, type, status: "connected" }]);
      setSuccess(`Source "${alias}" connected successfully`);
      setAlias("");
      setUri("");
      setTimeout(() => setSuccess(null), 3000);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to register source");
    } finally {
      setLoading(false);
    }
  };

  const handleRemoveSource = (aliasToRemove: string) => {
    setSources(sources.filter((s) => s.alias !== aliasToRemove));
  };

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold tracking-tight">Data Sources</h1>
        <p className="text-muted-foreground mt-2">
          Connect and manage your data sources for reconciliation
        </p>
      </div>

      {error && (
        <Alert variant="destructive">
          <AlertCircle className="h-4 w-4" />
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      {success && (
        <Alert>
          <CheckCircle className="h-4 w-4" />
          <AlertDescription>{success}</AlertDescription>
        </Alert>
      )}

      <div className="grid gap-6 md:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Add Data Source</CardTitle>
            <CardDescription>
              Upload files or connect to databases
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Tabs defaultValue="file">
              <TabsList className="grid w-full grid-cols-2">
                <TabsTrigger value="file">Upload File</TabsTrigger>
                <TabsTrigger value="connection">Connection String</TabsTrigger>
              </TabsList>

              <TabsContent value="file" className="space-y-4">
                <div className="border-2 border-dashed rounded-lg p-8 text-center">
                  <FileUp className="h-10 w-10 mx-auto text-muted-foreground mb-4" />
                  <p className="text-sm text-muted-foreground mb-4">
                    Drag and drop CSV or Parquet files, or click to browse
                  </p>
                  <Input
                    type="file"
                    accept=".csv,.parquet"
                    onChange={handleFileUpload}
                    className="max-w-xs mx-auto"
                  />
                </div>
              </TabsContent>

              <TabsContent value="connection" className="space-y-4">
                <div className="space-y-2">
                  <Label htmlFor="alias">Alias</Label>
                  <Input
                    id="alias"
                    placeholder="e.g., invoices, payments"
                    value={alias}
                    onChange={(e) => setAlias(e.target.value)}
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="uri">URI</Label>
                  <Input
                    id="uri"
                    placeholder="postgres://user:pass@host:5432/db?table=name"
                    value={uri}
                    onChange={(e) => setUri(e.target.value)}
                  />
                  <p className="text-xs text-muted-foreground">
                    Supported: file://, postgres://, s3:// (coming soon)
                  </p>
                </div>
                <Button onClick={handleAddSource} disabled={loading} className="w-full">
                  <Plus className="mr-2 h-4 w-4" />
                  {loading ? "Connecting..." : "Add Source"}
                </Button>
              </TabsContent>
            </Tabs>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Registered Sources</CardTitle>
            <CardDescription>
              {sources.length} source{sources.length !== 1 ? "s" : ""} registered
            </CardDescription>
          </CardHeader>
          <CardContent>
            {isLoading ? (
              <div className="text-center py-8 text-muted-foreground">
                <Loader2 className="h-10 w-10 mx-auto mb-4 animate-spin" />
                <p>Loading sources...</p>
              </div>
            ) : sources.length === 0 ? (
              <div className="text-center py-8 text-muted-foreground">
                <Database className="h-10 w-10 mx-auto mb-4 opacity-50" />
                <p>No sources registered yet</p>
                <p className="text-sm">Upload a file or add a connection to get started</p>
              </div>
            ) : (
              <div className="space-y-3">
                {sources.map((source) => (
                  <div
                    key={source.alias}
                    className="flex items-center justify-between p-3 rounded-lg border"
                  >
                    <div className="flex items-center gap-3">
                      <Database className="h-5 w-5 text-muted-foreground" />
                      <div>
                        <p className="font-medium">{source.alias}</p>
                        <p className="text-xs text-muted-foreground truncate max-w-[200px]">
                          {source.uri}
                        </p>
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      <Badge variant={source.type === "postgres" ? "default" : "secondary"}>
                        {source.type}
                      </Badge>
                      <Badge variant={source.status === "connected" ? "default" : "destructive"}>
                        {source.status}
                      </Badge>
                      <Button
                        variant="ghost"
                        size="icon"
                        onClick={() => handleRemoveSource(source.alias)}
                      >
                        <Trash2 className="h-4 w-4" />
                      </Button>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
