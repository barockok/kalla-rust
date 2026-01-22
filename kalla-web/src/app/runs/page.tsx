"use client";

import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { listRuns, RunSummary } from "@/lib/api";
import { History, RefreshCw, Eye } from "lucide-react";
import Link from "next/link";

export default function RunsPage() {
  const { data: runs, isLoading, refetch, isRefetching } = useQuery({
    queryKey: ["runs"],
    queryFn: listRuns,
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

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Run History</h1>
          <p className="text-muted-foreground mt-2">
            View past reconciliation runs and their results
          </p>
        </div>
        <Button variant="outline" onClick={() => refetch()} disabled={isRefetching}>
          <RefreshCw className={`mr-2 h-4 w-4 ${isRefetching ? "animate-spin" : ""}`} />
          Refresh
        </Button>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>All Runs</CardTitle>
          <CardDescription>
            {runs?.length || 0} reconciliation run{runs?.length !== 1 ? "s" : ""}
          </CardDescription>
        </CardHeader>
        <CardContent>
          {isLoading ? (
            <div className="text-center py-8 text-muted-foreground">
              Loading runs...
            </div>
          ) : !runs || runs.length === 0 ? (
            <div className="text-center py-8 text-muted-foreground">
              <History className="h-10 w-10 mx-auto mb-4 opacity-50" />
              <p>No reconciliation runs yet</p>
              <p className="text-sm">Start a new reconciliation to see results here</p>
              <Link href="/reconcile">
                <Button className="mt-4">New Reconciliation</Button>
              </Link>
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Run ID</TableHead>
                  <TableHead>Recipe</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead className="text-right">Matched</TableHead>
                  <TableHead className="text-right">Left Orphans</TableHead>
                  <TableHead className="text-right">Right Orphans</TableHead>
                  <TableHead>Started</TableHead>
                  <TableHead></TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {runs.map((run: RunSummary) => (
                  <TableRow key={run.run_id}>
                    <TableCell className="font-mono text-xs">
                      {run.run_id.slice(0, 8)}...
                    </TableCell>
                    <TableCell>{run.recipe_id}</TableCell>
                    <TableCell>{getStatusBadge(run.status)}</TableCell>
                    <TableCell className="text-right font-medium text-green-600">
                      {run.matched_count}
                    </TableCell>
                    <TableCell className="text-right text-orange-600">
                      {run.unmatched_left_count}
                    </TableCell>
                    <TableCell className="text-right text-orange-600">
                      {run.unmatched_right_count}
                    </TableCell>
                    <TableCell className="text-sm text-muted-foreground">
                      {formatDate(run.started_at)}
                    </TableCell>
                    <TableCell>
                      <Link href={`/runs/${run.run_id}`}>
                        <Button variant="ghost" size="sm">
                          <Eye className="h-4 w-4" />
                        </Button>
                      </Link>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
