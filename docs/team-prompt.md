# Kalla Release Team Prompt

> Paste this prompt into Claude Code to spawn a team that drives the kalla project to release readiness.

---

## Prompt

You are the team lead for the Kalla release. Kalla is a Universal Reconciliation Engine — Rust backend (DataFusion, Axum, 6 crates) + TypeScript/Next.js frontend with an agentic AI layer powered by Claude.

**Your mission:** Drive this project to production release. Every agent you spawn must operate independently, own their domain, and coordinate through the task list.

### Release Criteria (all must be green)

1. **AI agentic layer works end-to-end** — All 7 phases (greeting → intent → scoping → demonstration → inference → validation → execution) complete successfully in automated tests
2. **Rust dataflow works and scales** — DataFusion engine handles TB-scale data via streaming execution, worker/server split, and Ballista distributed execution
3. **95% test coverage** — Both Rust (`cargo llvm-cov`) and TypeScript (`jest --coverage`)
4. **Super reliable integration tests** — Full E2E reconciliation flows against real PostgreSQL, failure recovery, connector edge cases
5. **Release-ready** — Docker compose works, README accurate, no Priority 1 TODO items remaining

### Team Members

Create these 6 agents. Each agent works independently on their domain.

---

#### 1. **forge** — Rust Dataflow & Scaling Engineer

**Skills needed:** Rust, Apache DataFusion internals, Apache Arrow, Ballista, distributed systems, streaming execution, async Rust (tokio)

**Responsibilities:**
- Split `kalla-server` into API server + worker process for independent scaling
- Integrate Ballista for distributed DataFusion execution across multiple workers
- Implement streaming execution in `kalla-core/engine.rs` — never load full datasets into memory
- Add S3 connector in `kalla-connectors` using `object_store` crate with predicate pushdown
- Add BigQuery connector stub with the `SourceConnector` trait
- Implement partitioned execution — partition large reconciliation jobs by date/key ranges
- Add backpressure handling between connectors and the engine
- Ensure the `ReconciliationEngine` can process TB-scale datasets by streaming Arrow RecordBatches

**Key files:**
- `crates/kalla-core/src/engine.rs` — ReconciliationEngine
- `crates/kalla-connectors/src/postgres.rs` — PostgresConnector
- `crates/kalla-connectors/src/lib.rs` — SourceConnector trait
- `kalla-server/src/main.rs` — Server endpoints
- `Cargo.toml` — workspace dependencies

**Success criteria:**
- `cargo bench` shows constant memory usage regardless of input size
- Worker process runs independently from API server
- Ballista integration compiles and connects to a local cluster
- S3 connector reads Parquet files from MinIO in tests

---

#### 2. **aria** — AI Agentic Layer Engineer

**Skills needed:** TypeScript, LLM orchestration, state machines, Anthropic Claude API (tool_use), prompt engineering, Next.js API routes

**Responsibilities:**
- Harden the 7-phase state machine orchestrator in `agent.ts`
- Implement all missing agent tools: `propose_match`, `infer_rules`, `build_recipe`, `run_sample`, `run_full`
- Ensure context injections work correctly — schemas, samples, confirmed pairs injected into system prompt per phase
- Implement error recovery with retry budgets per the design doc
- Fix conversation history to preserve tool results across turns
- Implement `handleCardResponse` for match confirmation and validation approval flows
- Add structured error responses from Rust backend (error type + suggestion)
- Complete the demonstration → inference → validation → execution pipeline

**Key files:**
- `kalla-web/src/lib/agent.ts` — Orchestrator loop
- `kalla-web/src/lib/agent-tools.ts` — Tool definitions and execution
- `kalla-web/src/lib/chat-types.ts` — Session state, phase config
- `kalla-web/src/app/api/chat/route.ts` — Chat API route
- `docs/plans/2026-02-04-agentic-layer-state-machine-design.md` — Design spec (follow this)

**Success criteria:**
- Automated test walks through all 7 phases with mocked Claude responses
- Error recovery triggers after 2 failures and removes exhausted tools
- Phase transitions happen mid-turn when conditions are met
- Context injections include correct data for each phase

---

#### 3. **sentinel** — Rust Test Coverage Engineer

**Skills needed:** Rust testing, `cargo-llvm-cov`, property-based testing (proptest), test fixture design, async test patterns (tokio::test)

**Responsibilities:**
- Achieve 95% line coverage across all 6 Rust crates
- Add property-based tests for `kalla-recipe/transpiler.rs` — fuzz match conditions, edge cases
- Add unit tests for every public function in `kalla-core`, `kalla-connectors`, `kalla-recipe`, `kalla-evidence`, `kalla-ai`
- Test financial UDF `tolerance_match` with edge cases: NaN, infinity, negative amounts, zero, currency precision
- Test `FilterCondition` → SQL translation for all operators (eq, neq, gt, gte, lt, lte, between, in, like)
- Test `EvidenceStore` Parquet write/read round-trip with varied schemas
- Add error path tests — malformed recipes, missing columns, connection failures
- Set up `cargo llvm-cov` in CI and add coverage gate

**Key files:**
- All `crates/*/src/*.rs` files
- `kalla-server/tests/reconciliation_test.rs`
- New: `crates/*/tests/*.rs` integration test files

**Success criteria:**
- `cargo llvm-cov --workspace` reports >= 95% line coverage
- All tests pass with `cargo test --workspace`
- Property tests run 1000+ cases for transpiler
- Error paths are explicitly tested (not just happy paths)

---

#### 4. **weaver** — Frontend & UI Completion Engineer

**Skills needed:** TypeScript, React 19, Next.js 16, TailwindCSS, Radix UI, Jest, React Testing Library, responsive design

**Responsibilities:**
- Implement Priority 1 TODO items:
  - Result summary with match rate, unmatched counts, and issue flags
  - Live progress indicator during reconciliation runs (SSE or polling)
  - Field preview when configuring data sources
- Achieve 95% test coverage for all TypeScript/TSX files
- Add tests for every component in `src/components/`
- Add tests for all API client functions in `src/lib/api.ts`
- Add tests for the chat UI interaction flow
- Fix any accessibility issues (keyboard navigation, ARIA labels)
- Ensure responsive layout works on tablet and desktop

**Key files:**
- `kalla-web/src/components/*.tsx` — All UI components
- `kalla-web/src/app/*/page.tsx` — Page components
- `kalla-web/src/lib/api.ts` — API client
- `kalla-web/src/__tests__/*.test.ts(x)` — Test files
- `kalla-web/jest.config.ts`

**Success criteria:**
- `npx jest --coverage` reports >= 95% across all files
- All Priority 1 TODO items implemented and tested
- No TypeScript errors (`npx tsc --noEmit`)
- All components have corresponding test files

---

#### 5. **guardian** — Integration Testing & Reliability Engineer

**Skills needed:** Docker, PostgreSQL, Playwright, E2E testing, test fixture management, CI/CD (GitHub Actions), load testing

**Responsibilities:**
- Build comprehensive integration test suite for the full stack
- Create Docker-based test harness that spins up PostgreSQL + server + web for E2E
- Write Playwright E2E tests for the complete reconciliation flow:
  - Source registration → schema preview → scoping → matching → execution → results
- Test connector failure modes: PostgreSQL down, invalid credentials, timeout, schema mismatch
- Test recipe execution edge cases: empty sources, all-match, no-match, duplicate keys, null values
- Add load tests: 1M row reconciliation, concurrent runs, memory profiling
- Create test data generator (per TODO.md) — parameterized datasets with known match rates
- Set up GitHub Actions CI pipeline: lint → test → coverage gate → Docker build
- Test Docker compose cold-start: fresh `docker compose up` must work first try

**Key files:**
- `kalla-server/tests/reconciliation_test.rs` — Rust integration tests
- `kalla-web/e2e/` — Playwright tests
- `docker-compose.yml` — Docker configuration
- `scripts/init.sql` — Database schema
- `testdata/` — Test fixtures
- New: `.github/workflows/ci.yml`
- New: `scripts/generate-test-data.rs` or `scripts/generate-test-data.ts`

**Success criteria:**
- `docker compose up` works from clean state in CI
- Playwright tests cover the full reconciliation flow
- Integration tests cover all connector failure modes
- Load test confirms 1M row reconciliation completes without OOM
- CI pipeline runs in < 10 minutes

---

#### 6. **herald** — Release Engineering & Documentation

**Skills needed:** Technical writing, Docker optimization, semantic versioning, changelog management, API documentation, shell scripting

**Responsibilities:**
- Update README.md to reflect current state (new phases, scoping, state machine)
- Write API documentation for all endpoints (OpenAPI/Swagger spec)
- Create `CHANGELOG.md` with all features since inception
- Optimize Docker images (multi-stage builds, smaller base images)
- Create release checklist and versioning strategy
- Add `--version` flag to `kalla-cli`
- Create `scripts/release.sh` for automated release workflow
- Write deployment guide (Docker Compose for single-node, Ballista cluster for scale)
- Verify all environment variables are documented
- Add health check endpoints with dependency status (DB connected, worker available)
- Create sample recipes and demo data for first-time users

**Key files:**
- `README.md`
- `docker-compose.yml`
- `Cargo.toml` (version fields)
- `kalla-web/package.json` (version)
- New: `CHANGELOG.md`
- New: `docs/api-reference.md`
- New: `docs/deployment-guide.md`
- New: `scripts/release.sh`

**Success criteria:**
- README quickstart works for a new user with zero context
- API docs cover every endpoint with request/response examples
- Docker images are < 200MB
- `scripts/release.sh` tags, builds, and publishes without manual steps

---

### Task Dependencies

```
Phase 1 (parallel):
  forge: worker split + streaming execution
  aria: complete all 7 agentic phases
  sentinel: unit test coverage to 95%
  weaver: Priority 1 UI + frontend test coverage
  herald: documentation + Docker optimization

Phase 2 (after Phase 1):
  forge: Ballista integration + S3 connector
  guardian: full integration test suite (needs working server + frontend)
  sentinel: integration test coverage
  herald: release checklist

Phase 3 (after Phase 2):
  guardian: load testing + CI pipeline
  herald: final release prep
  ALL: bug fixes from integration testing
```

### Coordination Rules

1. All agents must read `CLAUDE.md`, `TODO.md`, and `docs/plans/` before starting work
2. When an agent changes an interface (Rust trait, TypeScript type, API endpoint), they must create a task notifying affected agents
3. `forge` and `aria` must coordinate on API contract changes (new endpoints, modified responses)
4. `sentinel` must not write implementation code — only tests. Flag missing functionality as tasks for the responsible agent.
5. `guardian` starts full integration work only after `forge` and `aria` confirm their domains are functional
6. All agents run `cargo test --workspace && cd kalla-web && npx jest` before marking any task complete

### Environment Setup

```bash
# Prerequisites
docker compose up -d postgres db-init  # PostgreSQL
cd kalla-server && cargo build         # Rust server
cd kalla-web && npm install && npm run dev  # Frontend

# Test commands
cargo test --workspace                 # Rust tests
cargo llvm-cov --workspace             # Rust coverage
cd kalla-web && npx jest --coverage    # Frontend tests
cd kalla-web && npx playwright test    # E2E tests
```

### Current State Summary

- **Working:** Core DataFusion engine, PostgreSQL connector, CSV/Parquet loading, recipe transpiler, evidence store, REST API, web UI navigation, state machine orchestrator (recently rewritten), markdown rendering
- **Incomplete:** Result summary UI, live progress, field preview, demonstration/inference/validation/execution phases, S3/BigQuery connectors, worker split, Ballista, comprehensive test suite
- **Recent changes:** State machine redesign (Feb 4-5), scoped loading, filter conditions, markdown rendering

Start by creating the task list with all work items, establish dependencies, then spawn agents and assign tasks. Coordinate through the shared task list. Report progress after each phase completes.
