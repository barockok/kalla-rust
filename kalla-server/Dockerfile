# Stage 1: Build
FROM rust:1.85-bookworm AS builder
WORKDIR /app

# Limit parallel compilation to reduce peak memory (~2-3GB instead of 8GB+)
ENV CARGO_BUILD_JOBS=2

# Copy workspace manifests only (no source code yet)
COPY Cargo.toml Cargo.lock ./
COPY crates/kalla-core/Cargo.toml crates/kalla-core/Cargo.toml
COPY crates/kalla-connectors/Cargo.toml crates/kalla-connectors/Cargo.toml
COPY crates/kalla-recipe/Cargo.toml crates/kalla-recipe/Cargo.toml
COPY crates/kalla-evidence/Cargo.toml crates/kalla-evidence/Cargo.toml
COPY crates/kalla-ai/Cargo.toml crates/kalla-ai/Cargo.toml
COPY crates/kalla-cli/Cargo.toml crates/kalla-cli/Cargo.toml
COPY kalla-server/Cargo.toml kalla-server/Cargo.toml

# Create dummy source files so cargo can resolve the workspace
RUN mkdir -p crates/kalla-core/src && echo "pub fn _dummy() {}" > crates/kalla-core/src/lib.rs && \
    mkdir -p crates/kalla-connectors/src && echo "pub fn _dummy() {}" > crates/kalla-connectors/src/lib.rs && \
    mkdir -p crates/kalla-recipe/src && echo "pub fn _dummy() {}" > crates/kalla-recipe/src/lib.rs && \
    mkdir -p crates/kalla-evidence/src && echo "pub fn _dummy() {}" > crates/kalla-evidence/src/lib.rs && \
    mkdir -p crates/kalla-ai/src && echo "pub fn _dummy() {}" > crates/kalla-ai/src/lib.rs && \
    mkdir -p crates/kalla-cli/src && echo "fn main() {}" > crates/kalla-cli/src/main.rs && \
    mkdir -p kalla-server/src && echo "fn main() {}" > kalla-server/src/main.rs

# Pin crates to versions compatible with Rust 1.85 (cached with dependencies)
RUN cargo update home --precise 0.5.9 && \
    cargo update comfy-table --precise 7.1.4

# Build dependencies only — cached between builds via BuildKit cache mounts
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release --bin kalla-server ; true

# Copy real source code (overwrites dummy stubs)
COPY crates ./crates
COPY kalla-server ./kalla-server

# Build with real source — only project crates recompile thanks to cached target dir
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release --bin kalla-server && \
    cp /app/target/release/kalla-server /usr/local/bin/kalla-server

# Stage 2: Runtime
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/kalla-server /usr/local/bin/
EXPOSE 3001
CMD ["kalla-server"]
