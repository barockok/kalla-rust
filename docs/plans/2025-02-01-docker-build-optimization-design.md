# Docker Build Optimization Design

## Problem

Rust builds inside Docker consume excessive memory, causing Docker Desktop OOM kills. Every code change triggers a full recompilation of all ~400 transitive dependencies (DataFusion, Arrow, etc.) because no dependency caching exists.

## Changes

### 1. Rewrite kalla-server/Dockerfile

Split the build into dependency pre-build and source build phases, both using BuildKit cache mounts:

- **Dependency phase**: Copy only Cargo.toml/Cargo.lock files + dummy source stubs. Build with `--mount=type=cache` on cargo registry and target dir. Dependencies compile once and persist between builds.
- **Source phase**: Copy real source over dummies, build again with same cache mounts. Only project crates recompile.
- **Memory limit**: `CARGO_BUILD_JOBS=2` caps parallel crate compilation at 2, reducing peak memory from ~8GB to ~2-3GB.
- **Pin crates before dependency build**: Move `cargo update` pins into the dependency phase so they are cached, not re-run every build.

### 2. Cargo.toml release profile

Add `codegen-units = 4` to reduce per-crate peak memory during code generation. Negligible binary size increase (~1-2%).

### 3. Docker Desktop recommendation

Set Docker Desktop memory to 6GB (Settings > Resources > Memory).

## What we are NOT doing

- No linker swap (mold/lld) — parallelism limit solves the memory issue
- No cargo-chef — BuildKit cache mounts achieve the same without extra tools
- No separate dev/release Docker profiles — one Dockerfile keeps it simple
- No LTO — not needed for dev, would increase memory and build time
