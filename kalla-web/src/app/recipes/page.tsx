"use client";

import { useState } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Textarea } from "@/components/ui/textarea";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { FileText, Plus, Play, Copy, CheckCircle, AlertCircle } from "lucide-react";
import { MatchRecipe, validateRecipe } from "@/lib/api";
import Link from "next/link";

// Sample recipes for demonstration
const sampleRecipes: MatchRecipe[] = [
  {
    version: "1.0",
    recipe_id: "invoice-payment-match",
    sources: {
      left: { alias: "invoices", uri: "file://invoices.csv" },
      right: { alias: "payments", uri: "file://payments.csv" },
    },
    match_rules: [
      {
        name: "id_and_amount_match",
        pattern: "1:1",
        conditions: [
          { left: "invoice_id", op: "eq", right: "payment_ref" },
          { left: "amount", op: "tolerance", right: "paid_amount", threshold: 0.01 },
        ],
      },
    ],
    output: {
      matched: "matched.parquet",
      unmatched_left: "unmatched_invoices.parquet",
      unmatched_right: "unmatched_payments.parquet",
    },
  },
];

export default function RecipesPage() {
  const [recipes] = useState<MatchRecipe[]>(sampleRecipes);
  const [selectedRecipe, setSelectedRecipe] = useState<MatchRecipe | null>(null);
  const [validationResult, setValidationResult] = useState<{ valid: boolean; errors: string[] } | null>(null);
  const [copied, setCopied] = useState(false);

  const handleValidate = async (recipe: MatchRecipe) => {
    try {
      const result = await validateRecipe(recipe);
      setValidationResult(result);
    } catch {
      setValidationResult({ valid: false, errors: ["Failed to validate recipe"] });
    }
  };

  const handleCopy = (recipe: MatchRecipe) => {
    navigator.clipboard.writeText(JSON.stringify(recipe, null, 2));
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Recipes</h1>
          <p className="text-muted-foreground mt-2">
            Browse and manage saved match recipes
          </p>
        </div>
        <Link href="/reconcile">
          <Button>
            <Plus className="mr-2 h-4 w-4" />
            Create Recipe
          </Button>
        </Link>
      </div>

      <div className="grid gap-6 md:grid-cols-2">
        {/* Recipe List */}
        <Card>
          <CardHeader>
            <CardTitle>Saved Recipes</CardTitle>
            <CardDescription>
              {recipes.length} recipe{recipes.length !== 1 ? "s" : ""} available
            </CardDescription>
          </CardHeader>
          <CardContent>
            {recipes.length === 0 ? (
              <div className="text-center py-8 text-muted-foreground">
                <FileText className="h-10 w-10 mx-auto mb-4 opacity-50" />
                <p>No saved recipes yet</p>
                <p className="text-sm">Generate a recipe from natural language to get started</p>
              </div>
            ) : (
              <div className="space-y-3">
                {recipes.map((recipe) => (
                  <div
                    key={recipe.recipe_id}
                    className={`p-4 rounded-lg border cursor-pointer transition-colors ${
                      selectedRecipe?.recipe_id === recipe.recipe_id
                        ? "border-primary bg-primary/5"
                        : "hover:border-primary/50"
                    }`}
                    onClick={() => {
                      setSelectedRecipe(recipe);
                      setValidationResult(null);
                    }}
                  >
                    <div className="flex items-center justify-between mb-2">
                      <h4 className="font-medium">{recipe.recipe_id}</h4>
                      <Badge variant="outline">v{recipe.version}</Badge>
                    </div>
                    <div className="flex gap-2 mb-2">
                      <Badge variant="secondary">{recipe.sources.left.alias}</Badge>
                      <span className="text-muted-foreground">↔</span>
                      <Badge variant="secondary">{recipe.sources.right.alias}</Badge>
                    </div>
                    <p className="text-xs text-muted-foreground">
                      {recipe.match_rules.length} rule{recipe.match_rules.length !== 1 ? "s" : ""}
                    </p>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        {/* Recipe Detail */}
        <Card>
          <CardHeader>
            <CardTitle>Recipe Details</CardTitle>
            <CardDescription>
              {selectedRecipe ? selectedRecipe.recipe_id : "Select a recipe to view details"}
            </CardDescription>
          </CardHeader>
          <CardContent>
            {!selectedRecipe ? (
              <div className="text-center py-8 text-muted-foreground">
                <FileText className="h-10 w-10 mx-auto mb-4 opacity-50" />
                <p>Select a recipe from the list</p>
              </div>
            ) : (
              <div className="space-y-4">
                {validationResult && (
                  <Alert variant={validationResult.valid ? "default" : "destructive"}>
                    {validationResult.valid ? (
                      <CheckCircle className="h-4 w-4" />
                    ) : (
                      <AlertCircle className="h-4 w-4" />
                    )}
                    <AlertDescription>
                      {validationResult.valid
                        ? "Recipe is valid"
                        : validationResult.errors.join(", ")}
                    </AlertDescription>
                  </Alert>
                )}

                <div>
                  <h4 className="text-sm font-medium mb-2">Match Rules</h4>
                  {selectedRecipe.match_rules.map((rule, i) => (
                    <div key={i} className="p-3 rounded-lg bg-muted mb-2">
                      <div className="flex items-center justify-between mb-1">
                        <span className="font-medium text-sm">{rule.name}</span>
                        <Badge variant="outline">{rule.pattern}</Badge>
                      </div>
                      <ul className="text-xs text-muted-foreground">
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

                <div>
                  <h4 className="text-sm font-medium mb-2">JSON</h4>
                  <Textarea
                    value={JSON.stringify(selectedRecipe, null, 2)}
                    readOnly
                    rows={10}
                    className="font-mono text-xs"
                  />
                </div>

                <div className="flex gap-2">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => handleCopy(selectedRecipe)}
                  >
                    {copied ? (
                      <CheckCircle className="mr-2 h-4 w-4" />
                    ) : (
                      <Copy className="mr-2 h-4 w-4" />
                    )}
                    {copied ? "Copied!" : "Copy JSON"}
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => handleValidate(selectedRecipe)}
                  >
                    <CheckCircle className="mr-2 h-4 w-4" />
                    Validate
                  </Button>
                  <Link href="/reconcile">
                    <Button size="sm">
                      <Play className="mr-2 h-4 w-4" />
                      Use Recipe
                    </Button>
                  </Link>
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
