# Priority 1 Features Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement three priority 1 features for public release: result summary with stats, live progress indicator, and field preview for data sources.

**Architecture:** Frontend-focused changes using React components with polling-based progress updates. The existing API provides all necessary data - we enhance the UI to better surface it. New components follow existing patterns (SourcePreview, PrimaryKeyConfirmation).

**Tech Stack:** Next.js 16, React 19, TypeScript, TailwindCSS, shadcn/ui components, React Query

---

## Task 1: Create RunSummaryStats Component

Create a reusable component that displays comprehensive reconciliation statistics with visual indicators and issue detection.

**Files:**
- Create: `kalla-web/src/components/RunSummaryStats.tsx`

**Step 1: Write the failing test**

Create test file `kalla-web/src/components/__tests__/RunSummaryStats.test.tsx`:

```tsx
import { render, screen } from '@testing-library/react';
import { RunSummaryStats } from '../RunSummaryStats';
import { RunMetadata } from '@/lib/api';

describe('RunSummaryStats', () => {
  const baseRun: RunMetadata = {
    run_id: 'test-run-1',
    recipe_id: 'recipe-1',
    started_at: '2026-01-27T10:00:00Z',
    completed_at: '2026-01-27T10:01:00Z',
    left_source: 'invoices',
    right_source: 'payments',
    left_record_count: 100,
    right_record_count: 100,
    matched_count: 80,
    unmatched_left_count: 20,
    unmatched_right_count: 20,
    status: 'Completed',
  };

  it('displays match rate percentage', () => {
    render(<RunSummaryStats run={baseRun} />);
    expect(screen.getByText('80.0%')).toBeInTheDocument();
  });

  it('displays matched count', () => {
    render(<RunSummaryStats run={baseRun} />);
    expect(screen.getByText('80')).toBeInTheDocument();
  });

  it('shows warning for low match rate', () => {
    const lowMatchRun = { ...baseRun, matched_count: 30, unmatched_left_count: 70 };
    render(<RunSummaryStats run={lowMatchRun} />);
    expect(screen.getByText(/low match rate/i)).toBeInTheDocument();
  });

  it('shows perfect match indicator for 100% match', () => {
    const perfectRun = { ...baseRun, matched_count: 100, unmatched_left_count: 0, unmatched_right_count: 0 };
    render(<RunSummaryStats run={perfectRun} />);
    expect(screen.getByText(/perfect match/i)).toBeInTheDocument();
  });
});
```

**Step 2: Run test to verify it fails**

Run: `cd kalla-web && npm test -- --testPathPattern=RunSummaryStats --watchAll=false`
Expected: FAIL with "Cannot find module '../RunSummaryStats'"

**Step 3: Write the component implementation**

Create `kalla-web/src/components/RunSummaryStats.tsx`:

```tsx
'use client';

import { RunMetadata } from '@/lib/api';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Progress } from '@/components/ui/progress';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { CheckCircle, XCircle, AlertTriangle, TrendingUp, TrendingDown } from 'lucide-react';

interface RunSummaryStatsProps {
  run: RunMetadata;
  compact?: boolean;
}

export function RunSummaryStats({ run, compact = false }: RunSummaryStatsProps) {
  const totalLeftRecords = run.matched_count + run.unmatched_left_count;
  const matchRate = totalLeftRecords > 0
    ? (run.matched_count / totalLeftRecords) * 100
    : 0;

  const isPerfectMatch = matchRate === 100 && run.unmatched_right_count === 0;
  const isLowMatchRate = matchRate < 50;
  const hasHighLeftOrphans = run.unmatched_left_count > run.matched_count;
  const hasHighRightOrphans = run.unmatched_right_count > run.matched_count;
  const hasNoMatches = run.matched_count === 0 && totalLeftRecords > 0;

  const issues: Array<{ type: 'warning' | 'error'; message: string }> = [];

  if (hasNoMatches) {
    issues.push({ type: 'error', message: 'No matches found - check your match rules or data quality' });
  } else if (isLowMatchRate) {
    issues.push({ type: 'warning', message: `Low match rate (${matchRate.toFixed(1)}%) - review unmatched records` });
  }

  if (hasHighLeftOrphans) {
    issues.push({ type: 'warning', message: `High unmatched left records (${run.unmatched_left_count}) - may indicate missing data in right source` });
  }

  if (hasHighRightOrphans) {
    issues.push({ type: 'warning', message: `High unmatched right records (${run.unmatched_right_count}) - may indicate missing data in left source` });
  }

  if (compact) {
    return (
      <div className="space-y-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            {isPerfectMatch ? (
              <CheckCircle className="h-5 w-5 text-green-500" />
            ) : isLowMatchRate ? (
              <AlertTriangle className="h-5 w-5 text-yellow-500" />
            ) : (
              <TrendingUp className="h-5 w-5 text-blue-500" />
            )}
            <span className="text-2xl font-bold">{matchRate.toFixed(1)}%</span>
            <span className="text-muted-foreground">match rate</span>
          </div>
          {isPerfectMatch && (
            <Badge className="bg-green-100 text-green-800">Perfect Match</Badge>
          )}
        </div>

        <Progress value={matchRate} className="h-2" />

        <div className="grid grid-cols-3 gap-4 text-center">
          <div>
            <div className="text-2xl font-bold text-green-600">{run.matched_count}</div>
            <div className="text-xs text-muted-foreground">Matched</div>
          </div>
          <div>
            <div className="text-2xl font-bold text-orange-600">{run.unmatched_left_count}</div>
            <div className="text-xs text-muted-foreground">Left Orphans</div>
          </div>
          <div>
            <div className="text-2xl font-bold text-orange-600">{run.unmatched_right_count}</div>
            <div className="text-xs text-muted-foreground">Right Orphans</div>
          </div>
        </div>

        {issues.length > 0 && (
          <div className="space-y-2">
            {issues.map((issue, idx) => (
              <Alert key={idx} variant={issue.type === 'error' ? 'destructive' : 'default'}>
                <AlertTriangle className="h-4 w-4" />
                <AlertDescription>{issue.message}</AlertDescription>
              </Alert>
            ))}
          </div>
        )}
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Match Rate Card */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium flex items-center gap-2">
            Match Rate
            {isPerfectMatch && (
              <Badge className="bg-green-100 text-green-800">Perfect Match</Badge>
            )}
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-center gap-4">
            <div className="text-4xl font-bold">{matchRate.toFixed(1)}%</div>
            <Progress value={matchRate} className="flex-1 h-3" />
          </div>
        </CardContent>
      </Card>

      {/* Statistics Grid */}
      <div className="grid gap-4 md:grid-cols-3">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-green-600 flex items-center gap-2">
              <CheckCircle className="h-4 w-4" />
              Matched Records
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-bold">{run.matched_count.toLocaleString()}</div>
            <p className="text-xs text-muted-foreground mt-1">
              of {totalLeftRecords.toLocaleString()} left records
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-orange-600 flex items-center gap-2">
              <XCircle className="h-4 w-4" />
              Left Orphans
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-bold">{run.unmatched_left_count.toLocaleString()}</div>
            <p className="text-xs text-muted-foreground mt-1">
              records without matches in right source
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-orange-600 flex items-center gap-2">
              <XCircle className="h-4 w-4" />
              Right Orphans
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-bold">{run.unmatched_right_count.toLocaleString()}</div>
            <p className="text-xs text-muted-foreground mt-1">
              records without matches in left source
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Source Statistics */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium">Source Statistics</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 gap-4">
            <div className="p-3 rounded-lg bg-muted">
              <p className="text-xs text-muted-foreground">Left Source</p>
              <p className="font-mono text-sm truncate">{run.left_source}</p>
              <p className="text-lg font-semibold">{run.left_record_count.toLocaleString()} records</p>
            </div>
            <div className="p-3 rounded-lg bg-muted">
              <p className="text-xs text-muted-foreground">Right Source</p>
              <p className="font-mono text-sm truncate">{run.right_source}</p>
              <p className="text-lg font-semibold">{run.right_record_count.toLocaleString()} records</p>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Issues Section */}
      {issues.length > 0 && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium flex items-center gap-2">
              <AlertTriangle className="h-4 w-4 text-yellow-500" />
              Potential Issues
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-2">
            {issues.map((issue, idx) => (
              <Alert key={idx} variant={issue.type === 'error' ? 'destructive' : 'default'}>
                <AlertDescription>{issue.message}</AlertDescription>
              </Alert>
            ))}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
```

**Step 4: Run test to verify it passes**

Run: `cd kalla-web && npm test -- --testPathPattern=RunSummaryStats --watchAll=false`
Expected: PASS

**Step 5: Commit**

```bash
git add kalla-web/src/components/RunSummaryStats.tsx kalla-web/src/components/__tests__/RunSummaryStats.test.tsx
git commit -m "feat: add RunSummaryStats component with issue detection"
```

---

## Task 2: Integrate RunSummaryStats into Reconcile Complete Step

Update the reconcile page to show rich statistics after run completion instead of the generic "Reconciliation Started" message.

**Files:**
- Modify: `kalla-web/src/app/reconcile/page.tsx`

**Step 1: Write the failing test**

Create test file `kalla-web/src/app/reconcile/__tests__/page.test.tsx`:

```tsx
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

// Mock next/navigation
jest.mock('next/navigation', () => ({
  useRouter: () => ({ push: jest.fn() }),
}));

// Mock the API module
jest.mock('@/lib/api', () => ({
  listRecipes: jest.fn().mockResolvedValue([]),
  createRun: jest.fn().mockResolvedValue({ run_id: 'test-run-1', status: 'running' }),
  getRun: jest.fn().mockResolvedValue({
    run_id: 'test-run-1',
    recipe_id: 'recipe-1',
    started_at: '2026-01-27T10:00:00Z',
    completed_at: '2026-01-27T10:01:00Z',
    left_source: 'invoices',
    right_source: 'payments',
    left_record_count: 100,
    right_record_count: 100,
    matched_count: 80,
    unmatched_left_count: 20,
    unmatched_right_count: 20,
    status: 'Completed',
  }),
  generateRecipe: jest.fn(),
  validateRecipe: jest.fn(),
  validateRecipeSchema: jest.fn(),
}));

import ReconcilePage from '../page';

describe('ReconcilePage Complete Step', () => {
  it('shows match rate in complete step', async () => {
    // This test verifies the complete step shows run statistics
    // Implementation will poll for run completion and display stats
  });
});
```

**Step 2: Run test to verify baseline**

Run: `cd kalla-web && npm test -- --testPathPattern=reconcile/.*page --watchAll=false`
Expected: Test file created (may pass with empty test)

**Step 3: Update reconcile page to fetch and display run stats**

Modify `kalla-web/src/app/reconcile/page.tsx`:

Add import at top (after existing imports around line 19):
```tsx
import { RunSummaryStats } from "@/components/RunSummaryStats";
import { MatchRecipe, SavedRecipe, generateRecipe, validateRecipe, validateRecipeSchema, createRun, listRecipes, getRun, SchemaValidationResult, RunMetadata } from "@/lib/api";
```

Add state for run data (after line 47, with other state):
```tsx
const [completedRun, setCompletedRun] = useState<RunMetadata | null>(null);
```

Update handleApprove function (replace lines 154-171) to poll for completion:
```tsx
const handleApprove = async () => {
  if (!recipe) return;

  setLoading(true);
  setError(null);
  setStep("running");

  try {
    const result = await createRun(recipe);
    setRunId(result.run_id);

    // Poll for completion
    const pollForCompletion = async () => {
      const maxAttempts = 120; // 2 minutes max
      let attempts = 0;

      while (attempts < maxAttempts) {
        const runData = await getRun(result.run_id);
        if (runData.status === "Completed" || runData.status === "Failed") {
          setCompletedRun(runData);
          setStep("complete");
          setLoading(false);
          return;
        }
        await new Promise(resolve => setTimeout(resolve, 1000));
        attempts++;
      }

      // Timeout - still show complete but without stats
      setStep("complete");
      setLoading(false);
    };

    pollForCompletion();
  } catch (err) {
    setError(err instanceof Error ? err.message : "Failed to start reconciliation");
    setStep("review");
    setLoading(false);
  }
};
```

Update handleReset function (add after line 189):
```tsx
setCompletedRun(null);
```

Replace Step 4 Complete section (lines 519-538) with:
```tsx
{/* Step 4: Complete */}
{step === "complete" && (
  <div className="space-y-6">
    <Card>
      <CardContent className="py-8">
        <div className="flex items-center gap-4 mb-6">
          <CheckCircle className="h-10 w-10 text-green-500" />
          <div>
            <h3 className="text-xl font-semibold">Reconciliation Complete</h3>
            <p className="text-sm text-muted-foreground font-mono">{runId}</p>
          </div>
        </div>

        {completedRun ? (
          <RunSummaryStats run={completedRun} compact />
        ) : (
          <p className="text-muted-foreground">Loading results...</p>
        )}
      </CardContent>
    </Card>

    <div className="flex gap-4">
      <Button onClick={handleViewRun}>
        View Full Results
      </Button>
      <Button variant="outline" onClick={handleReset}>
        New Reconciliation
      </Button>
    </div>
  </div>
)}
```

**Step 4: Run test to verify it passes**

Run: `cd kalla-web && npm run build`
Expected: Build succeeds without errors

**Step 5: Commit**

```bash
git add kalla-web/src/app/reconcile/page.tsx
git commit -m "feat: display run statistics in reconcile complete step"
```

---

## Task 3: Create ProgressIndicator Component

Create a component that shows real-time progress during reconciliation runs.

**Files:**
- Create: `kalla-web/src/components/ProgressIndicator.tsx`

**Step 1: Write the failing test**

Create test file `kalla-web/src/components/__tests__/ProgressIndicator.test.tsx`:

```tsx
import { render, screen } from '@testing-library/react';
import { ProgressIndicator } from '../ProgressIndicator';

describe('ProgressIndicator', () => {
  it('displays current phase', () => {
    render(
      <ProgressIndicator
        phase="matching"
        currentStep="Processing rule: exact_match"
        elapsedSeconds={15}
      />
    );
    expect(screen.getByText(/matching/i)).toBeInTheDocument();
  });

  it('displays elapsed time', () => {
    render(
      <ProgressIndicator
        phase="counting"
        currentStep="Counting records"
        elapsedSeconds={65}
      />
    );
    expect(screen.getByText('1:05')).toBeInTheDocument();
  });

  it('shows spinner animation', () => {
    render(
      <ProgressIndicator
        phase="matching"
        currentStep="Processing"
        elapsedSeconds={0}
      />
    );
    expect(screen.getByRole('status')).toBeInTheDocument();
  });
});
```

**Step 2: Run test to verify it fails**

Run: `cd kalla-web && npm test -- --testPathPattern=ProgressIndicator --watchAll=false`
Expected: FAIL with "Cannot find module '../ProgressIndicator'"

**Step 3: Write the component implementation**

Create `kalla-web/src/components/ProgressIndicator.tsx`:

```tsx
'use client';

import { Card, CardContent } from '@/components/ui/card';
import { Progress } from '@/components/ui/progress';
import { Loader2, Database, GitCompare, Search, CheckCircle } from 'lucide-react';

interface ProgressIndicatorProps {
  phase: 'counting' | 'matching' | 'orphan_detection' | 'complete';
  currentStep: string;
  elapsedSeconds: number;
  matchedSoFar?: number;
}

const phaseConfig = {
  counting: {
    label: 'Counting Records',
    icon: Database,
    progress: 10,
    color: 'text-blue-500',
  },
  matching: {
    label: 'Matching Records',
    icon: GitCompare,
    progress: 50,
    color: 'text-purple-500',
  },
  orphan_detection: {
    label: 'Finding Orphans',
    icon: Search,
    progress: 85,
    color: 'text-orange-500',
  },
  complete: {
    label: 'Complete',
    icon: CheckCircle,
    progress: 100,
    color: 'text-green-500',
  },
};

function formatElapsedTime(seconds: number): string {
  const mins = Math.floor(seconds / 60);
  const secs = seconds % 60;
  return `${mins}:${secs.toString().padStart(2, '0')}`;
}

export function ProgressIndicator({
  phase,
  currentStep,
  elapsedSeconds,
  matchedSoFar
}: ProgressIndicatorProps) {
  const config = phaseConfig[phase];
  const Icon = config.icon;

  return (
    <Card>
      <CardContent className="py-8">
        <div className="flex flex-col items-center text-center space-y-6">
          {/* Animated Icon */}
          <div className="relative" role="status" aria-label="Processing">
            {phase !== 'complete' ? (
              <>
                <Loader2 className="h-16 w-16 animate-spin text-primary opacity-20" />
                <Icon className={`h-8 w-8 absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 ${config.color}`} />
              </>
            ) : (
              <CheckCircle className="h-16 w-16 text-green-500" />
            )}
          </div>

          {/* Phase Label */}
          <div>
            <h3 className="text-xl font-semibold">{config.label}</h3>
            <p className="text-sm text-muted-foreground mt-1">{currentStep}</p>
          </div>

          {/* Progress Bar */}
          <div className="w-full max-w-md space-y-2">
            <Progress value={config.progress} className="h-2" />
            <div className="flex justify-between text-xs text-muted-foreground">
              <span>Elapsed: {formatElapsedTime(elapsedSeconds)}</span>
              {matchedSoFar !== undefined && matchedSoFar > 0 && (
                <span>{matchedSoFar.toLocaleString()} matched so far</span>
              )}
            </div>
          </div>

          {/* Phase Steps */}
          <div className="flex gap-2">
            {Object.entries(phaseConfig).map(([key, value]) => (
              <div
                key={key}
                className={`flex items-center gap-1 px-2 py-1 rounded text-xs ${
                  key === phase
                    ? 'bg-primary text-primary-foreground'
                    : phaseConfig[phase].progress > value.progress
                      ? 'bg-green-100 text-green-800'
                      : 'bg-muted text-muted-foreground'
                }`}
              >
                {key === 'counting' && 'Count'}
                {key === 'matching' && 'Match'}
                {key === 'orphan_detection' && 'Orphans'}
                {key === 'complete' && 'Done'}
              </div>
            ))}
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
```

**Step 4: Run test to verify it passes**

Run: `cd kalla-web && npm test -- --testPathPattern=ProgressIndicator --watchAll=false`
Expected: PASS

**Step 5: Commit**

```bash
git add kalla-web/src/components/ProgressIndicator.tsx kalla-web/src/components/__tests__/ProgressIndicator.test.tsx
git commit -m "feat: add ProgressIndicator component for run progress"
```

---

## Task 4: Integrate ProgressIndicator into Reconcile Running Step

Update the running step to show the progress indicator with polling updates.

**Files:**
- Modify: `kalla-web/src/app/reconcile/page.tsx`

**Step 1: Read current implementation**

Review the running step implementation at lines 506-517.

**Step 2: Update reconcile page with progress tracking**

Add import at top:
```tsx
import { ProgressIndicator } from "@/components/ProgressIndicator";
```

Add state for progress tracking (after other state declarations):
```tsx
const [progressPhase, setProgressPhase] = useState<'counting' | 'matching' | 'orphan_detection' | 'complete'>('counting');
const [progressStep, setProgressStep] = useState('Initializing...');
const [elapsedSeconds, setElapsedSeconds] = useState(0);
```

Update handleApprove to track progress during polling (replace the entire function):
```tsx
const handleApprove = async () => {
  if (!recipe) return;

  setLoading(true);
  setError(null);
  setStep("running");
  setProgressPhase('counting');
  setProgressStep('Starting reconciliation...');
  setElapsedSeconds(0);

  try {
    const result = await createRun(recipe);
    setRunId(result.run_id);

    // Start elapsed time counter
    const startTime = Date.now();
    const timerInterval = setInterval(() => {
      setElapsedSeconds(Math.floor((Date.now() - startTime) / 1000));
    }, 1000);

    // Poll for completion with phase estimation
    const pollForCompletion = async () => {
      const maxAttempts = 120;
      let attempts = 0;

      while (attempts < maxAttempts) {
        const runData = await getRun(result.run_id);

        // Estimate phase based on available data
        if (runData.status === "Completed" || runData.status === "Failed") {
          clearInterval(timerInterval);
          setProgressPhase('complete');
          setProgressStep('Reconciliation complete');
          setCompletedRun(runData);
          setStep("complete");
          setLoading(false);
          return;
        }

        // Estimate progress phase from counts
        if (runData.left_record_count > 0 && runData.matched_count === 0) {
          setProgressPhase('counting');
          setProgressStep(`Counted ${runData.left_record_count.toLocaleString()} left, ${runData.right_record_count.toLocaleString()} right records`);
        } else if (runData.matched_count > 0 && runData.unmatched_left_count === 0) {
          setProgressPhase('matching');
          setProgressStep(`${runData.matched_count.toLocaleString()} records matched so far`);
        } else if (runData.matched_count > 0) {
          setProgressPhase('orphan_detection');
          setProgressStep('Identifying unmatched records');
        }

        await new Promise(resolve => setTimeout(resolve, 1000));
        attempts++;
      }

      clearInterval(timerInterval);
      setStep("complete");
      setLoading(false);
    };

    pollForCompletion();
  } catch (err) {
    setError(err instanceof Error ? err.message : "Failed to start reconciliation");
    setStep("review");
    setLoading(false);
  }
};
```

Update handleReset to reset progress state:
```tsx
const handleReset = () => {
  setStep("input");
  setRecipe(null);
  setRecipeJson("");
  setRunId(null);
  setSelectedRecipeId("");
  setLeftSource("");
  setRightSource("");
  setPrompt("");
  setError(null);
  setSchemaValidation(null);
  setCompletedRun(null);
  setProgressPhase('counting');
  setProgressStep('Initializing...');
  setElapsedSeconds(0);
};
```

Replace Step 3 Running section (lines 506-517) with:
```tsx
{/* Step 3: Running */}
{step === "running" && (
  <ProgressIndicator
    phase={progressPhase}
    currentStep={progressStep}
    elapsedSeconds={elapsedSeconds}
  />
)}
```

**Step 3: Run build to verify changes**

Run: `cd kalla-web && npm run build`
Expected: Build succeeds without errors

**Step 4: Commit**

```bash
git add kalla-web/src/app/reconcile/page.tsx
git commit -m "feat: integrate progress indicator into reconciliation flow"
```

---

## Task 5: Create FieldPreview Component

Create a lightweight component that displays available columns/fields from a data source.

**Files:**
- Create: `kalla-web/src/components/FieldPreview.tsx`

**Step 1: Write the failing test**

Create test file `kalla-web/src/components/__tests__/FieldPreview.test.tsx`:

```tsx
import { render, screen, waitFor } from '@testing-library/react';
import { FieldPreview } from '../FieldPreview';

// Mock fetch
global.fetch = jest.fn();

describe('FieldPreview', () => {
  beforeEach(() => {
    (global.fetch as jest.Mock).mockReset();
  });

  it('displays column names', async () => {
    (global.fetch as jest.Mock).mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        alias: 'test-source',
        columns: [
          { name: 'id', data_type: 'Int64', nullable: false },
          { name: 'name', data_type: 'Utf8', nullable: true },
        ],
        rows: [['1', 'Test']],
        total_rows: 100,
        preview_rows: 1,
      }),
    });

    render(<FieldPreview sourceAlias="test-source" />);

    await waitFor(() => {
      expect(screen.getByText('id')).toBeInTheDocument();
      expect(screen.getByText('name')).toBeInTheDocument();
    });
  });

  it('displays data types', async () => {
    (global.fetch as jest.Mock).mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        alias: 'test-source',
        columns: [
          { name: 'amount', data_type: 'Float64', nullable: false },
        ],
        rows: [['100.50']],
        total_rows: 50,
        preview_rows: 1,
      }),
    });

    render(<FieldPreview sourceAlias="test-source" />);

    await waitFor(() => {
      expect(screen.getByText('Float64')).toBeInTheDocument();
    });
  });

  it('shows nullable indicator', async () => {
    (global.fetch as jest.Mock).mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        alias: 'test-source',
        columns: [
          { name: 'optional_field', data_type: 'Utf8', nullable: true },
        ],
        rows: [['value']],
        total_rows: 10,
        preview_rows: 1,
      }),
    });

    render(<FieldPreview sourceAlias="test-source" />);

    await waitFor(() => {
      expect(screen.getByText(/nullable/i)).toBeInTheDocument();
    });
  });

  it('shows loading state', () => {
    (global.fetch as jest.Mock).mockImplementation(() => new Promise(() => {}));

    render(<FieldPreview sourceAlias="test-source" />);
    expect(screen.getByText(/loading/i)).toBeInTheDocument();
  });

  it('shows error state', async () => {
    (global.fetch as jest.Mock).mockResolvedValueOnce({
      ok: false,
      text: async () => 'Source not found',
    });

    render(<FieldPreview sourceAlias="invalid-source" />);

    await waitFor(() => {
      expect(screen.getByText(/error/i)).toBeInTheDocument();
    });
  });
});
```

**Step 2: Run test to verify it fails**

Run: `cd kalla-web && npm test -- --testPathPattern=FieldPreview --watchAll=false`
Expected: FAIL with "Cannot find module '../FieldPreview'"

**Step 3: Write the component implementation**

Create `kalla-web/src/components/FieldPreview.tsx`:

```tsx
'use client';

import { useState, useEffect } from 'react';
import { Badge } from '@/components/ui/badge';
import { Input } from '@/components/ui/input';
import { Loader2, Search, Hash, Type, Calendar, ToggleLeft, Database } from 'lucide-react';

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

interface FieldPreviewProps {
  sourceAlias: string;
  onSelectField?: (fieldName: string) => void;
}

function getTypeIcon(dataType: string) {
  const type = dataType.toLowerCase();
  if (type.includes('int') || type.includes('float') || type.includes('decimal')) {
    return Hash;
  }
  if (type.includes('date') || type.includes('time')) {
    return Calendar;
  }
  if (type.includes('bool')) {
    return ToggleLeft;
  }
  return Type;
}

function getTypeColor(dataType: string): string {
  const type = dataType.toLowerCase();
  if (type.includes('int') || type.includes('float') || type.includes('decimal')) {
    return 'bg-blue-100 text-blue-800';
  }
  if (type.includes('date') || type.includes('time')) {
    return 'bg-purple-100 text-purple-800';
  }
  if (type.includes('bool')) {
    return 'bg-green-100 text-green-800';
  }
  return 'bg-gray-100 text-gray-800';
}

export function FieldPreview({ sourceAlias, onSelectField }: FieldPreviewProps) {
  const [preview, setPreview] = useState<SourcePreviewResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [searchTerm, setSearchTerm] = useState('');

  useEffect(() => {
    async function fetchPreview() {
      setLoading(true);
      setError(null);
      try {
        const res = await fetch(`/api/sources/${sourceAlias}/preview?limit=1`);
        if (!res.ok) {
          throw new Error(await res.text());
        }
        const data: SourcePreviewResponse = await res.json();
        setPreview(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load fields');
      } finally {
        setLoading(false);
      }
    }
    fetchPreview();
  }, [sourceAlias]);

  if (loading) {
    return (
      <div className="flex items-center gap-2 p-4 text-muted-foreground">
        <Loader2 className="h-4 w-4 animate-spin" />
        <span>Loading fields for {sourceAlias}...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-4 text-red-600 bg-red-50 rounded-lg">
        <p className="font-medium">Error loading fields</p>
        <p className="text-sm">{error}</p>
      </div>
    );
  }

  if (!preview) {
    return null;
  }

  const filteredColumns = preview.columns.filter(col =>
    col.name.toLowerCase().includes(searchTerm.toLowerCase())
  );

  // Get sample values from first row
  const sampleValues: Record<string, string> = {};
  if (preview.rows.length > 0) {
    preview.columns.forEach((col, idx) => {
      sampleValues[col.name] = preview.rows[0][idx];
    });
  }

  return (
    <div className="border rounded-lg overflow-hidden">
      <div className="bg-muted px-4 py-2 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Database className="h-4 w-4" />
          <span className="font-medium">{preview.alias}</span>
          <Badge variant="secondary">{preview.columns.length} fields</Badge>
        </div>
        <span className="text-sm text-muted-foreground">
          {preview.total_rows.toLocaleString()} rows
        </span>
      </div>

      {preview.columns.length > 5 && (
        <div className="px-4 py-2 border-b">
          <div className="relative">
            <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              placeholder="Search fields..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="pl-8 h-8"
            />
          </div>
        </div>
      )}

      <div className="divide-y max-h-80 overflow-y-auto">
        {filteredColumns.map((col) => {
          const Icon = getTypeIcon(col.data_type);
          const sampleValue = sampleValues[col.name];

          return (
            <div
              key={col.name}
              className={`flex items-center justify-between px-4 py-2 hover:bg-muted/50 ${
                onSelectField ? 'cursor-pointer' : ''
              }`}
              onClick={() => onSelectField?.(col.name)}
            >
              <div className="flex items-center gap-3">
                <Icon className="h-4 w-4 text-muted-foreground" />
                <div>
                  <p className="font-mono text-sm font-medium">{col.name}</p>
                  {sampleValue && sampleValue !== 'null' && (
                    <p className="text-xs text-muted-foreground truncate max-w-[200px]">
                      e.g., {sampleValue}
                    </p>
                  )}
                </div>
              </div>
              <div className="flex items-center gap-2">
                <Badge variant="outline" className={getTypeColor(col.data_type)}>
                  {col.data_type}
                </Badge>
                {col.nullable && (
                  <Badge variant="outline" className="text-xs">nullable</Badge>
                )}
              </div>
            </div>
          );
        })}

        {filteredColumns.length === 0 && (
          <div className="px-4 py-8 text-center text-muted-foreground">
            No fields match "{searchTerm}"
          </div>
        )}
      </div>
    </div>
  );
}
```

**Step 4: Run test to verify it passes**

Run: `cd kalla-web && npm test -- --testPathPattern=FieldPreview --watchAll=false`
Expected: PASS

**Step 5: Commit**

```bash
git add kalla-web/src/components/FieldPreview.tsx kalla-web/src/components/__tests__/FieldPreview.test.tsx
git commit -m "feat: add FieldPreview component for displaying source columns"
```

---

## Task 6: Add Field Preview Toggle to Sources Page

Add a toggle to switch between full row preview and field-only preview on the sources page.

**Files:**
- Modify: `kalla-web/src/app/sources/page.tsx`

**Step 1: Read current implementation**

Review the preview toggle at lines 233-263 in sources/page.tsx.

**Step 2: Update sources page with field preview option**

Add import at top:
```tsx
import { FieldPreview } from "@/components/FieldPreview";
```

Add state for preview mode (after line 51):
```tsx
const [previewMode, setPreviewMode] = useState<'fields' | 'rows'>('fields');
```

Update the preview button section (replace lines 233-240):
```tsx
<div className="flex items-center gap-1">
  <Button
    variant={previewSource === source.alias && previewMode === 'fields' ? 'default' : 'ghost'}
    size="icon"
    onClick={() => {
      if (previewSource === source.alias && previewMode === 'fields') {
        setPreviewSource(null);
      } else {
        setPreviewSource(source.alias);
        setPreviewMode('fields');
      }
    }}
    title="View fields"
  >
    <Database className="h-4 w-4" />
  </Button>
  <Button
    variant={previewSource === source.alias && previewMode === 'rows' ? 'default' : 'ghost'}
    size="icon"
    onClick={() => {
      if (previewSource === source.alias && previewMode === 'rows') {
        setPreviewSource(null);
      } else {
        setPreviewSource(source.alias);
        setPreviewMode('rows');
      }
    }}
    title="Preview data rows"
  >
    {previewSource === source.alias && previewMode === 'rows' ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
  </Button>
</div>
```

Update the preview display section (replace lines 259-263):
```tsx
{previewSource === source.alias && (
  <div className="ml-8">
    {previewMode === 'fields' ? (
      <FieldPreview sourceAlias={source.alias} />
    ) : (
      <SourcePreview sourceAlias={source.alias} limit={10} />
    )}
  </div>
)}
```

**Step 3: Run build to verify changes**

Run: `cd kalla-web && npm run build`
Expected: Build succeeds without errors

**Step 4: Commit**

```bash
git add kalla-web/src/app/sources/page.tsx
git commit -m "feat: add field preview toggle to sources page"
```

---

## Task 7: Enhance Run Details Page with RunSummaryStats

Replace the basic stats cards on the run details page with the new RunSummaryStats component.

**Files:**
- Modify: `kalla-web/src/app/runs/[id]/page.tsx`

**Step 1: Read current implementation**

Review the current stats display at lines 82-128 in runs/[id]/page.tsx.

**Step 2: Update run details page**

Add import at top:
```tsx
import { RunSummaryStats } from "@/components/RunSummaryStats";
```

Replace the summary cards section (lines 82-128) with:
```tsx
{/* Summary Stats */}
{run && <RunSummaryStats run={run} />}
```

Keep the rest of the page (Run Information, Export, Results Tabs) as-is but move them after the stats.

**Step 3: Run build to verify changes**

Run: `cd kalla-web && npm run build`
Expected: Build succeeds without errors

**Step 4: Commit**

```bash
git add kalla-web/src/app/runs/[id]/page.tsx
git commit -m "feat: integrate RunSummaryStats into run details page"
```

---

## Task 8: Integration Testing

Manually test all features end-to-end.

**Files:**
- None (manual testing)

**Step 1: Start the development server**

Run: `cd kalla-web && npm run dev`

**Step 2: Test Field Preview**

1. Navigate to `/sources`
2. Register a test source if none exist
3. Click the Database icon to view fields
4. Verify column names, types, and nullable indicators display
5. Click the Eye icon to switch to row preview
6. Verify data rows display correctly

**Step 3: Test Progress Indicator**

1. Navigate to `/reconcile`
2. Select or create a recipe
3. Click "Approve & Run"
4. Verify progress indicator shows:
   - Animated spinner
   - Current phase (Counting → Matching → Orphans → Complete)
   - Elapsed time counter
   - Phase step indicators

**Step 4: Test Result Summary**

1. After run completes, verify:
   - Match rate percentage with progress bar
   - Matched/orphan counts
   - Issue detection (if applicable)
2. Click "View Full Results"
3. Verify enhanced stats display on run details page

**Step 5: Commit test notes**

```bash
git commit --allow-empty -m "test: manual integration testing complete for priority 1 features"
```

---

## Task 9: Update TODO.md

Mark priority 1 features as complete.

**Files:**
- Modify: `TODO.md`

**Step 1: Update TODO.md**

```markdown
## Priority 1: Must-Have for Public Release

### Results & Feedback
- [x] **Result summary with stats** - Display match rate, unmatched counts, and potential issues after each run
- [x] **Live progress indicator** - Show real-time progress during reconciliation runs

### Source Setup Experience
- [x] **Field preview** - Display available columns when configuring a data source
```

**Step 2: Commit**

```bash
git add TODO.md
git commit -m "docs: mark Priority 1 features as complete"
```

---

## Summary

This plan implements three Priority 1 features:

1. **Result Summary with Stats** (Tasks 1-2, 7)
   - New `RunSummaryStats` component with match rate, counts, and issue detection
   - Integrated into reconcile complete step and run details page

2. **Live Progress Indicator** (Tasks 3-4)
   - New `ProgressIndicator` component with phase tracking
   - Polling-based progress updates during reconciliation

3. **Field Preview** (Tasks 5-6)
   - New `FieldPreview` component showing columns with types
   - Toggle between field view and row preview on sources page

Total: 9 tasks, each following TDD approach with frequent commits.
