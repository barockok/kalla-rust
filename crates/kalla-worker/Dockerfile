# Stage 1: Build
FROM rust:1.85-bookworm AS builder
WORKDIR /app

ENV CARGO_BUILD_JOBS=2

COPY Cargo.toml Cargo.lock ./
COPY crates/kalla-core/Cargo.toml crates/kalla-core/Cargo.toml
COPY crates/kalla-connectors/Cargo.toml crates/kalla-connectors/Cargo.toml
COPY crates/kalla-recipe/Cargo.toml crates/kalla-recipe/Cargo.toml
COPY crates/kalla-evidence/Cargo.toml crates/kalla-evidence/Cargo.toml
COPY crates/kalla-ai/Cargo.toml crates/kalla-ai/Cargo.toml
COPY crates/kalla-cli/Cargo.toml crates/kalla-cli/Cargo.toml
COPY crates/kalla-worker/Cargo.toml crates/kalla-worker/Cargo.toml
COPY kalla-server/Cargo.toml kalla-server/Cargo.toml

RUN mkdir -p crates/kalla-core/src && echo "pub fn _dummy() {}" > crates/kalla-core/src/lib.rs && \
    mkdir -p crates/kalla-connectors/src && echo "pub fn _dummy() {}" > crates/kalla-connectors/src/lib.rs && \
    mkdir -p crates/kalla-recipe/src && echo "pub fn _dummy() {}" > crates/kalla-recipe/src/lib.rs && \
    mkdir -p crates/kalla-evidence/src && echo "pub fn _dummy() {}" > crates/kalla-evidence/src/lib.rs && \
    mkdir -p crates/kalla-ai/src && echo "pub fn _dummy() {}" > crates/kalla-ai/src/lib.rs && \
    mkdir -p crates/kalla-cli/src && echo "fn main() {}" > crates/kalla-cli/src/main.rs && \
    mkdir -p crates/kalla-worker/src && echo "fn main() {}" > crates/kalla-worker/src/main.rs && \
    mkdir -p kalla-server/src && echo "fn main() {}" > kalla-server/src/main.rs

RUN cargo update home --precise 0.5.9 && \
    cargo update comfy-table --precise 7.1.4

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --release --bin kalla-worker 2>&1 || true

COPY crates ./crates
COPY kalla-server ./kalla-server

RUN find /app/crates -name '*.rs' -exec touch {} + && \
    find /app/kalla-server -name '*.rs' -exec touch {} +

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --release --bin kalla-worker && \
    cp target/release/kalla-worker /usr/local/bin/kalla-worker

# Stage 2: Runtime
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libssl3 ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/kalla-worker /usr/local/bin/
EXPOSE 9090
CMD ["kalla-worker"]
