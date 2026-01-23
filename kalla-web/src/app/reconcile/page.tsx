"use client";

import { useState, useEffect } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Wand2, Play, Edit, CheckCircle, AlertCircle, Loader2, FileText } from "lucide-react";
import { MatchRecipe, SavedRecipe, generateRecipe, validateRecipe, createRun, listRecipes } from "@/lib/api";
import { useRouter } from "next/navigation";

type Step = "input" | "review" | "running" | "complete";
type Mode = "existing" | "new";

export default function ReconcilePage() {
  const router = useRouter();
  const [step, setStep] = useState<Step>("input");
  const [mode, setMode] = useState<Mode>("existing");

  // Existing recipe selection state
  const [savedRecipes, setSavedRecipes] = useState<SavedRecipe[]>([]);
  const [selectedRecipeId, setSelectedRecipeId] = useState<string>("");
  const [loadingRecipes, setLoadingRecipes] = useState(true);

  // New recipe generation state
  const [leftSource, setLeftSource] = useState("");
  const [rightSource, setRightSource] = useState("");
  const [prompt, setPrompt] = useState("");

  // Common state
  const [recipe, setRecipe] = useState<MatchRecipe | null>(null);
  const [recipeJson, setRecipeJson] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [runId, setRunId] = useState<string | null>(null);

  // Fetch saved recipes on mount
  useEffect(() => {
    async function fetchRecipes() {
      try {
        const recipes = await listRecipes();
        setSavedRecipes(recipes);
        if (recipes.length === 0) {
          setMode("new");
        }
      } catch (err) {
        console.error("Failed to fetch recipes:", err);
      } finally {
        setLoadingRecipes(false);
      }
    }
    fetchRecipes();
  }, []);

  const handleSelectRecipe = () => {
    if (!selectedRecipeId) {
      setError("Please select a recipe");
      return;
    }

    const selected = savedRecipes.find((r) => r.recipe_id === selectedRecipeId);
    if (!selected) {
      setError("Recipe not found");
      return;
    }

    setRecipe(selected.config);
    setRecipeJson(JSON.stringify(selected.config, null, 2));
    setError(null);
    setStep("review");
  };

  const handleGenerate = async () => {
    if (!leftSource || !rightSource || !prompt) {
      setError("Please fill in all fields");
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const result = await generateRecipe(leftSource, rightSource, prompt);
      if (result.error) {
        setError(result.error);
      } else if (result.recipe) {
        setRecipe(result.recipe);
        setRecipeJson(JSON.stringify(result.recipe, null, 2));
        setStep("review");
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to generate recipe");
    } finally {
      setLoading(false);
    }
  };

  const handleValidate = async () => {
    try {
      const parsed = JSON.parse(recipeJson);
      const result = await validateRecipe(parsed);
      if (!result.valid) {
        setError(`Validation errors: ${result.errors.join(", ")}`);
      } else {
        setRecipe(parsed);
        setError(null);
      }
    } catch (err) {
      setError("Invalid JSON format");
    }
  };

  const handleApprove = async () => {
    if (!recipe) return;

    setLoading(true);
    setError(null);
    setStep("running");

    try {
      const result = await createRun(recipe);
      setRunId(result.run_id);
      setStep("complete");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to start reconciliation");
      setStep("review");
    } finally {
      setLoading(false);
    }
  };

  const handleViewRun = () => {
    if (runId) {
      router.push(`/runs/${runId}`);
    }
  };

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
  };

  const selectedRecipe = savedRecipes.find((r) => r.recipe_id === selectedRecipeId);

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold tracking-tight">New Reconciliation</h1>
        <p className="text-muted-foreground mt-2">
          Use an existing recipe or create a new one
        </p>
      </div>

      {/* Progress Steps */}
      <div className="flex items-center gap-4">
        {["input", "review", "running", "complete"].map((s, i) => (
          <div key={s} className="flex items-center">
            <div
              className={`flex h-8 w-8 items-center justify-center rounded-full text-sm font-medium ${
                step === s
                  ? "bg-primary text-primary-foreground"
                  : ["input", "review", "running", "complete"].indexOf(step) > i
                    ? "bg-green-500 text-white"
                    : "bg-muted text-muted-foreground"
              }`}
            >
              {["input", "review", "running", "complete"].indexOf(step) > i ? (
                <CheckCircle className="h-4 w-4" />
              ) : (
                i + 1
              )}
            </div>
            {i < 3 && <div className="h-px w-16 bg-border ml-2" />}
          </div>
        ))}
      </div>

      {error && (
        <Alert variant="destructive">
          <AlertCircle className="h-4 w-4" />
          <AlertTitle>Error</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      {/* Step 1: Input */}
      {step === "input" && (
        <Card>
          <CardHeader>
            <CardTitle>Select Recipe</CardTitle>
            <CardDescription>
              Choose an existing recipe or create a new one with AI
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Tabs value={mode} onValueChange={(v) => setMode(v as Mode)}>
              <TabsList className="grid w-full grid-cols-2 mb-6">
                <TabsTrigger value="existing" disabled={loadingRecipes || savedRecipes.length === 0}>
                  <FileText className="mr-2 h-4 w-4" />
                  Use Existing Recipe
                </TabsTrigger>
                <TabsTrigger value="new">
                  <Wand2 className="mr-2 h-4 w-4" />
                  Create New Recipe
                </TabsTrigger>
              </TabsList>

              {/* Use Existing Recipe Tab */}
              <TabsContent value="existing" className="space-y-6">
                {loadingRecipes ? (
                  <div className="flex items-center justify-center py-8">
                    <Loader2 className="h-6 w-6 animate-spin mr-2" />
                    Loading recipes...
                  </div>
                ) : savedRecipes.length === 0 ? (
                  <div className="text-center py-8 text-muted-foreground">
                    <p>No saved recipes found.</p>
                    <p className="text-sm mt-2">Create a new recipe to get started.</p>
                  </div>
                ) : (
                  <>
                    <div className="space-y-2">
                      <Label htmlFor="recipe-select">Select Recipe</Label>
                      <Select value={selectedRecipeId} onValueChange={setSelectedRecipeId}>
                        <SelectTrigger>
                          <SelectValue placeholder="Choose a saved recipe..." />
                        </SelectTrigger>
                        <SelectContent>
                          {savedRecipes.map((r) => (
                            <SelectItem key={r.recipe_id} value={r.recipe_id}>
                              {r.name}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>

                    {selectedRecipe && (
                      <div className="p-4 rounded-lg border bg-muted/50">
                        <h4 className="font-medium mb-2">{selectedRecipe.name}</h4>
                        {selectedRecipe.description && (
                          <p className="text-sm text-muted-foreground mb-3">
                            {selectedRecipe.description}
                          </p>
                        )}
                        <div className="flex gap-2">
                          <Badge variant="outline">
                            {selectedRecipe.config.sources.left.alias}
                          </Badge>
                          <span className="text-muted-foreground">↔</span>
                          <Badge variant="outline">
                            {selectedRecipe.config.sources.right.alias}
                          </Badge>
                        </div>
                        <p className="text-xs text-muted-foreground mt-2">
                          {selectedRecipe.config.match_rules.length} match rule(s)
                        </p>
                      </div>
                    )}

                    <Button onClick={handleSelectRecipe} disabled={!selectedRecipeId}>
                      <Play className="mr-2 h-4 w-4" />
                      Continue with Selected Recipe
                    </Button>
                  </>
                )}
              </TabsContent>

              {/* Create New Recipe Tab */}
              <TabsContent value="new" className="space-y-6">
                <div className="grid gap-4 md:grid-cols-2">
                  <div className="space-y-2">
                    <Label htmlFor="left">Left Source (alias)</Label>
                    <Input
                      id="left"
                      placeholder="e.g., invoices"
                      value={leftSource}
                      onChange={(e) => setLeftSource(e.target.value)}
                    />
                    <p className="text-xs text-muted-foreground">
                      The alias of your registered left data source
                    </p>
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="right">Right Source (alias)</Label>
                    <Input
                      id="right"
                      placeholder="e.g., payments"
                      value={rightSource}
                      onChange={(e) => setRightSource(e.target.value)}
                    />
                    <p className="text-xs text-muted-foreground">
                      The alias of your registered right data source
                    </p>
                  </div>
                </div>

                <div className="space-y-2">
                  <Label htmlFor="prompt">Matching Instructions</Label>
                  <Textarea
                    id="prompt"
                    placeholder="Describe how records should be matched. For example:&#10;&#10;Match invoices to payments by invoice_id = payment_ref, allowing a 1 cent tolerance on the amount field."
                    value={prompt}
                    onChange={(e) => setPrompt(e.target.value)}
                    rows={5}
                  />
                  <p className="text-xs text-muted-foreground">
                    Use natural language to describe the matching logic. The AI will generate a recipe for your approval.
                  </p>
                </div>

                <Button onClick={handleGenerate} disabled={loading}>
                  {loading ? (
                    <>
                      <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                      Generating...
                    </>
                  ) : (
                    <>
                      <Wand2 className="mr-2 h-4 w-4" />
                      Generate Recipe
                    </>
                  )}
                </Button>
              </TabsContent>
            </Tabs>
          </CardContent>
        </Card>
      )}

      {/* Step 2: Review */}
      {step === "review" && recipe && (
        <div className="grid gap-6 md:grid-cols-2">
          <Card>
            <CardHeader>
              <CardTitle>Recipe Summary</CardTitle>
              <CardDescription>Review the matching rules before running</CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div>
                <p className="text-sm font-medium">Recipe ID</p>
                <p className="text-sm text-muted-foreground">{recipe.recipe_id}</p>
              </div>

              <div>
                <p className="text-sm font-medium">Sources</p>
                <div className="flex gap-2 mt-1">
                  <Badge variant="outline">{recipe.sources.left.alias}</Badge>
                  <span className="text-muted-foreground">↔</span>
                  <Badge variant="outline">{recipe.sources.right.alias}</Badge>
                </div>
              </div>

              <div>
                <p className="text-sm font-medium mb-2">Match Rules</p>
                {recipe.match_rules.map((rule, i) => (
                  <div key={i} className="p-3 rounded-lg border mb-2">
                    <div className="flex items-center justify-between mb-2">
                      <span className="font-medium">{rule.name}</span>
                      <Badge>{rule.pattern}</Badge>
                    </div>
                    <ul className="text-sm text-muted-foreground space-y-1">
                      {rule.conditions.map((cond, j) => (
                        <li key={j}>
                          {cond.left} {cond.op} {cond.right}
                          {cond.threshold !== undefined && ` (±${cond.threshold})`}
                        </li>
                      ))}
                    </ul>
                  </div>
                ))}
              </div>

              <div className="flex gap-2">
                <Button onClick={handleApprove} disabled={loading}>
                  <CheckCircle className="mr-2 h-4 w-4" />
                  Approve & Run
                </Button>
                <Button variant="outline" onClick={() => setStep("input")}>
                  Back
                </Button>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Recipe JSON</CardTitle>
              <CardDescription>Edit the recipe directly if needed</CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <Textarea
                value={recipeJson}
                onChange={(e) => setRecipeJson(e.target.value)}
                rows={20}
                className="font-mono text-xs"
              />
              <Button variant="outline" onClick={handleValidate}>
                <Edit className="mr-2 h-4 w-4" />
                Validate Changes
              </Button>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Step 3: Running */}
      {step === "running" && (
        <Card>
          <CardContent className="py-12 text-center">
            <Loader2 className="h-12 w-12 animate-spin mx-auto mb-4 text-primary" />
            <h3 className="text-lg font-medium mb-2">Running Reconciliation</h3>
            <p className="text-muted-foreground">
              Processing your data. This may take a few moments...
            </p>
          </CardContent>
        </Card>
      )}

      {/* Step 4: Complete */}
      {step === "complete" && (
        <Card>
          <CardContent className="py-12 text-center">
            <CheckCircle className="h-12 w-12 mx-auto mb-4 text-green-500" />
            <h3 className="text-lg font-medium mb-2">Reconciliation Started</h3>
            <p className="text-muted-foreground mb-6">
              Run ID: {runId}
            </p>
            <div className="flex justify-center gap-4">
              <Button onClick={handleViewRun}>
                View Results
              </Button>
              <Button variant="outline" onClick={handleReset}>
                New Reconciliation
              </Button>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
