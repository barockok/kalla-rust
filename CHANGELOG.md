# Changelog

All notable changes to Kalla (Universal Reconciliation Engine) will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Markdown rendering in agent chat messages with rich text display via MarkdownRenderer component.
- Tailwind Typography plugin for styled prose content.
- react-markdown dependency for parsing and rendering markdown in the web UI.

### Fixed

- ES2017-compatible regex in react-markdown mock for broader runtime support.

## [0.1.0] - 2026-02-11

### Added

- Core reconciliation engine powered by Apache DataFusion for SQL-based record matching.
- REST API built on Axum with endpoints for reconciliation runs, sources, recipes, and schema operations.
- Next.js web UI with a conversational agentic recipe builder replacing the manual reconcile page.
- 7-phase state machine orchestrator driving the agentic workflow: greeting, intent, scoping, demonstration, inference, validation, and execution.
- Declarative state machine configuration replacing hardcoded phase types.
- Scoped data loading with structured filter conditions and SQL WHERE clause builder.
- AI/LLM-powered recipe generation via Anthropic Claude SDK integration.
- Ollama alternative backend for local LLM inference via OpenAI-compatible API.
- Primary key detection with heuristic analysis and dedicated API endpoint.
- Primary key confirmation UI component for user verification.
- Schema validation for recipes against source schemas with API endpoint.
- Smart field name resolution with normalization for fuzzy column matching.
- Row preview API endpoint and UI component for inspecting data source contents.
- Multi-format data source connectors: CSV, Parquet, and PostgreSQL.
- Dynamic alias detection so CSV sources are matched correctly at query time.
- On-demand re-registration of CSV/Parquet sources with DataFusion.
- Register-scoped connector method for PostgreSQL with filter support.
- Match operations: eq, tolerance, gt, lt, gte, lte, contains, startswith, endswith.
- Match patterns: 1:1, 1:N, M:1 for flexible cardinality matching.
- Evidence store with full audit trail tracking matched and unmatched records.
- CLI tool with reconcile, validate-recipe, generate-recipe, and report commands.
- Chat and session management API routes for the agentic interface.
- Chat UI components for the conversational recipe builder.
- Seed data for bootstrapping development and demo environments.
- Integration tests for Priority 2 features (preview, primary key, schema validation).
- Playwright E2E test configuration and test scenarios for the agentic recipe builder.
- TODO.md and instruction.md project planning documents.

### Changed

- Replaced manual reconcile page with conversational agentic interface.
- Replaced phase type enums with declarative state machine configuration.
- Replaced load_sample tool with load_scoped tool for filtered data loading.
- Simplified API routes to thin pass-through handlers.
- Added new session fields to support state machine transitions.

### Fixed

- Schema corrections for database models.
- Idempotent init.sql allowing safe re-runs without errors.
- Required environment variables passed to web service in Docker Compose.
- SERVER_API_URL configured for server-side backend calls within the Docker network.
- Dynamic alias detection for correct CSV source matching.
- On-demand re-registration of CSV/Parquet sources with DataFusion.
- FilterCondition import and type narrowing in agent TypeScript code.
- Code review findings addressed across both TypeScript and Rust codebases.
- Hardened E2E tests with graceful error handling for the agentic recipe builder.

### Infrastructure

- Docker Compose setup with PostgreSQL 16 and multi-service orchestration.
- Dedicated db-init service guaranteeing schema availability on every startup.
- Optimized Docker build with BuildKit cache mounts and parallelism limits.
- Added .worktrees/ to .gitignore.
