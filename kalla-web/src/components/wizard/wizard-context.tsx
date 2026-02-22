"use client";

import { createContext, useContext, useReducer, type ReactNode, type Dispatch } from "react";
import {
  type WizardState,
  type WizardAction,
  INITIAL_WIZARD_STATE,
} from "@/lib/wizard-types";

function wizardReducer(state: WizardState, action: WizardAction): WizardState {
  switch (action.type) {
    case "SET_STEP":
      return { ...state, step: action.step };
    case "SET_SOURCES":
      return { ...state, leftSource: action.left, rightSource: action.right };
    case "SET_SCHEMAS":
      return {
        ...state,
        schemaLeft: action.schemaLeft,
        schemaRight: action.schemaRight,
        previewLeft: action.previewLeft,
        previewRight: action.previewRight,
      };
    case "SET_FIELD_MAPPINGS":
      return {
        ...state,
        fieldMappings: action.mappings,
        suggestedFilters: action.suggestedFilters,
      };
    case "SET_COMMON_FILTERS":
      return { ...state, commonFilters: action.filters };
    case "UPDATE_COMMON_FILTER":
      return {
        ...state,
        commonFilters: state.commonFilters.map((f) =>
          f.id === action.id ? { ...f, ...action.updates } : f,
        ),
      };
    case "SET_NL_TEXT":
      return { ...state, nlFilterText: action.text };
    case "SET_NL_RESULT":
      return {
        ...state,
        commonFilters: action.filters,
        nlFilterExplanation: action.explanation,
      };
    case "SET_SOURCE_FILTERS_LEFT":
      return { ...state, sourceFiltersLeft: action.filters };
    case "SET_SOURCE_FILTERS_RIGHT":
      return { ...state, sourceFiltersRight: action.filters };
    case "SET_SAMPLE":
      return action.side === "left"
        ? { ...state, sampleLeft: action.data }
        : { ...state, sampleRight: action.data };
    case "SET_LOADING":
      return { ...state, loading: { ...state.loading, [action.key]: action.value } };
    case "SET_ERROR":
      return { ...state, errors: { ...state.errors, [action.key]: action.error } };
    case "SET_INFERRED_RULES":
      return {
        ...state,
        detectedPattern: action.pattern,
        primaryKeys: action.primaryKeys,
        inferredRules: action.rules,
      };
    case "ACCEPT_RULE":
      return {
        ...state,
        inferredRules: state.inferredRules.map((r) =>
          r.id === action.id ? { ...r, status: "accepted" as const } : r,
        ),
      };
    case "REJECT_RULE":
      return {
        ...state,
        inferredRules: state.inferredRules.map((r) =>
          r.id === action.id ? { ...r, status: "rejected" as const } : r,
        ),
      };
    case "ADD_CUSTOM_RULE":
      return {
        ...state,
        inferredRules: [...state.inferredRules, action.rule],
      };
    case "SET_RECIPE_SQL":
      return { ...state, builtRecipeSql: action.sql };
    default:
      return state;
  }
}

interface WizardContextValue {
  state: WizardState;
  dispatch: Dispatch<WizardAction>;
}

const WizardContext = createContext<WizardContextValue | null>(null);

export function WizardProvider({ children }: { children: ReactNode }) {
  const [state, dispatch] = useReducer(wizardReducer, INITIAL_WIZARD_STATE);
  return (
    <WizardContext.Provider value={{ state, dispatch }}>
      {children}
    </WizardContext.Provider>
  );
}

export function useWizard(): WizardContextValue {
  const ctx = useContext(WizardContext);
  if (!ctx) throw new Error("useWizard must be used within WizardProvider");
  return ctx;
}
