# Stage 1: Build
FROM rust:1.85-bookworm AS builder
WORKDIR /app

# Which binary to build: kalla-scheduler or kalla-executor
ARG BINARY=kalla-scheduler

ENV CARGO_BUILD_JOBS=2

COPY Cargo.toml Cargo.lock ./
COPY crates/kalla-core/Cargo.toml crates/kalla-core/Cargo.toml
COPY crates/kalla-connectors/Cargo.toml crates/kalla-connectors/Cargo.toml
COPY crates/kalla-recipe/Cargo.toml crates/kalla-recipe/Cargo.toml
COPY crates/kalla-evidence/Cargo.toml crates/kalla-evidence/Cargo.toml
COPY crates/kalla-worker/Cargo.toml crates/kalla-worker/Cargo.toml
COPY crates/kalla-ballista/Cargo.toml crates/kalla-ballista/Cargo.toml

RUN mkdir -p crates/kalla-core/src && echo "pub fn _dummy() {}" > crates/kalla-core/src/lib.rs && \
    mkdir -p crates/kalla-connectors/src && echo "pub fn _dummy() {}" > crates/kalla-connectors/src/lib.rs && \
    mkdir -p crates/kalla-recipe/src && echo "pub fn _dummy() {}" > crates/kalla-recipe/src/lib.rs && \
    mkdir -p crates/kalla-evidence/src && echo "pub fn _dummy() {}" > crates/kalla-evidence/src/lib.rs && \
    mkdir -p crates/kalla-worker/src && echo "fn main() {}" > crates/kalla-worker/src/main.rs && \
    mkdir -p crates/kalla-ballista/src/bin && echo "pub fn _dummy() {}" > crates/kalla-ballista/src/lib.rs && \
    echo "fn main() {}" > crates/kalla-ballista/src/bin/kalla-scheduler.rs && \
    echo "fn main() {}" > crates/kalla-ballista/src/bin/kalla-executor.rs

RUN cargo update home --precise 0.5.9 && \
    cargo update comfy-table --precise 7.1.4 && \
    cargo update time --precise 0.3.36 && \
    cargo update time-core --precise 0.1.2

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --release --bin ${BINARY} 2>&1 || true

COPY crates ./crates

RUN find /app/crates -name '*.rs' -exec touch {} +

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --release --bin ${BINARY} && \
    cp target/release/${BINARY} /usr/local/bin/${BINARY}

# Stage 2: Runtime
FROM debian:bookworm-slim
ARG BINARY=kalla-scheduler
RUN apt-get update && apt-get install -y --no-install-recommends libssl3 ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/${BINARY} /usr/local/bin/entrypoint
EXPOSE 50050
CMD ["entrypoint"]
