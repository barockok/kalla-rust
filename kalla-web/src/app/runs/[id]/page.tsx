"use client";

import { useQuery } from "@tanstack/react-query";
import { useParams } from "next/navigation";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { getRun, RunMetadata } from "@/lib/api";
import { ArrowLeft, Download, CheckCircle, XCircle, Clock, FileDown } from "lucide-react";
import Link from "next/link";

export default function RunDetailPage() {
  const params = useParams();
  const runId = params.id as string;

  const { data: run, isLoading, error } = useQuery({
    queryKey: ["run", runId],
    queryFn: () => getRun(runId),
    enabled: !!runId,
  });

  const formatDate = (dateStr: string) => {
    return new Date(dateStr).toLocaleString();
  };

  const getStatusBadge = (status: string) => {
    switch (status.toLowerCase()) {
      case "completed":
        return <Badge className="bg-green-500">Completed</Badge>;
      case "running":
        return <Badge className="bg-blue-500">Running</Badge>;
      case "failed":
        return <Badge variant="destructive">Failed</Badge>;
      default:
        return <Badge variant="secondary">{status}</Badge>;
    }
  };

  const calculateMatchRate = (run: RunMetadata) => {
    const total = run.matched_count + run.unmatched_left_count;
    if (total === 0) return 0;
    return ((run.matched_count / total) * 100).toFixed(1);
  };

  if (isLoading) {
    return (
      <div className="text-center py-12">
        <p className="text-muted-foreground">Loading run details...</p>
      </div>
    );
  }

  if (error || !run) {
    return (
      <div className="text-center py-12">
        <p className="text-red-500">Failed to load run details</p>
        <Link href="/runs">
          <Button variant="outline" className="mt-4">
            <ArrowLeft className="mr-2 h-4 w-4" />
            Back to Runs
          </Button>
        </Link>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-4">
        <Link href="/runs">
          <Button variant="ghost" size="icon">
            <ArrowLeft className="h-4 w-4" />
          </Button>
        </Link>
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Run Details</h1>
          <p className="text-muted-foreground mt-1 font-mono text-sm">{runId}</p>
        </div>
      </div>

      {/* Summary Cards */}
      <div className="grid gap-4 md:grid-cols-4">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">Status</CardTitle>
          </CardHeader>
          <CardContent>
            {getStatusBadge(run.status)}
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-green-600">Matched</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-center gap-2">
              <CheckCircle className="h-5 w-5 text-green-500" />
              <span className="text-2xl font-bold">{run.matched_count}</span>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-orange-600">Left Orphans</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-center gap-2">
              <XCircle className="h-5 w-5 text-orange-500" />
              <span className="text-2xl font-bold">{run.unmatched_left_count}</span>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-orange-600">Right Orphans</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-center gap-2">
              <XCircle className="h-5 w-5 text-orange-500" />
              <span className="text-2xl font-bold">{run.unmatched_right_count}</span>
            </div>
          </CardContent>
        </Card>
      </div>

      <div className="grid gap-6 md:grid-cols-2">
        {/* Run Info */}
        <Card>
          <CardHeader>
            <CardTitle>Run Information</CardTitle>
            <CardDescription>Details about this reconciliation run</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div>
                <p className="text-sm font-medium">Recipe ID</p>
                <p className="text-sm text-muted-foreground">{run.recipe_id}</p>
              </div>
              <div>
                <p className="text-sm font-medium">Match Rate</p>
                <p className="text-sm text-muted-foreground">{calculateMatchRate(run)}%</p>
              </div>
              <div>
                <p className="text-sm font-medium">Started</p>
                <p className="text-sm text-muted-foreground">{formatDate(run.started_at)}</p>
              </div>
              <div>
                <p className="text-sm font-medium">Completed</p>
                <p className="text-sm text-muted-foreground">
                  {run.completed_at ? formatDate(run.completed_at) : "In progress"}
                </p>
              </div>
            </div>

            <div className="pt-4 border-t">
              <p className="text-sm font-medium mb-2">Data Sources</p>
              <div className="space-y-2">
                <div className="flex items-center justify-between p-2 rounded bg-muted">
                  <span className="text-sm">Left</span>
                  <span className="text-xs text-muted-foreground font-mono truncate max-w-[200px]">
                    {run.left_source}
                  </span>
                </div>
                <div className="flex items-center justify-between p-2 rounded bg-muted">
                  <span className="text-sm">Right</span>
                  <span className="text-xs text-muted-foreground font-mono truncate max-w-[200px]">
                    {run.right_source}
                  </span>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Export */}
        <Card>
          <CardHeader>
            <CardTitle>Export Results</CardTitle>
            <CardDescription>Download reconciliation results</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <p className="text-sm text-muted-foreground">
              Export matched and unmatched records for further analysis or review.
            </p>

            <div className="space-y-2">
              <Button variant="outline" className="w-full justify-start" disabled>
                <FileDown className="mr-2 h-4 w-4" />
                Download Matched Records (Parquet)
                <Badge variant="secondary" className="ml-auto">{run.matched_count}</Badge>
              </Button>

              <Button variant="outline" className="w-full justify-start" disabled>
                <FileDown className="mr-2 h-4 w-4" />
                Download Left Orphans (Parquet)
                <Badge variant="secondary" className="ml-auto">{run.unmatched_left_count}</Badge>
              </Button>

              <Button variant="outline" className="w-full justify-start" disabled>
                <FileDown className="mr-2 h-4 w-4" />
                Download Right Orphans (Parquet)
                <Badge variant="secondary" className="ml-auto">{run.unmatched_right_count}</Badge>
              </Button>
            </div>

            <p className="text-xs text-muted-foreground">
              Export functionality coming soon. Results are available in the evidence directory.
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Results Tabs */}
      <Card>
        <CardHeader>
          <CardTitle>Results Preview</CardTitle>
          <CardDescription>Preview of matched and unmatched records</CardDescription>
        </CardHeader>
        <CardContent>
          <Tabs defaultValue="matched">
            <TabsList>
              <TabsTrigger value="matched">
                Matched ({run.matched_count})
              </TabsTrigger>
              <TabsTrigger value="left-orphans">
                Left Orphans ({run.unmatched_left_count})
              </TabsTrigger>
              <TabsTrigger value="right-orphans">
                Right Orphans ({run.unmatched_right_count})
              </TabsTrigger>
            </TabsList>

            <TabsContent value="matched" className="py-4">
              <div className="text-center py-8 text-muted-foreground">
                <CheckCircle className="h-10 w-10 mx-auto mb-4 text-green-500 opacity-50" />
                <p>{run.matched_count} records matched successfully</p>
                <p className="text-sm">
                  Preview functionality coming soon. Download the Parquet file to view details.
                </p>
              </div>
            </TabsContent>

            <TabsContent value="left-orphans" className="py-4">
              <div className="text-center py-8 text-muted-foreground">
                <XCircle className="h-10 w-10 mx-auto mb-4 text-orange-500 opacity-50" />
                <p>{run.unmatched_left_count} records from left source without matches</p>
                <p className="text-sm">
                  These records did not find matching records in the right source.
                </p>
              </div>
            </TabsContent>

            <TabsContent value="right-orphans" className="py-4">
              <div className="text-center py-8 text-muted-foreground">
                <XCircle className="h-10 w-10 mx-auto mb-4 text-orange-500 opacity-50" />
                <p>{run.unmatched_right_count} records from right source without matches</p>
                <p className="text-sm">
                  These records did not find matching records in the left source.
                </p>
              </div>
            </TabsContent>
          </Tabs>
        </CardContent>
      </Card>
    </div>
  );
}
