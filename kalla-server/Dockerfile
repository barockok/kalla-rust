# Stage 1: Build
FROM rust:1.85-bookworm AS builder
WORKDIR /app

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY kalla-server ./kalla-server

# Pin crates to versions compatible with Rust 1.85
RUN cargo update home --precise 0.5.9 && \
    cargo update comfy-table --precise 7.1.4

# Build the release binary
RUN cargo build --release --bin kalla-server

# Stage 2: Runtime
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/kalla-server /usr/local/bin/
EXPOSE 3001
CMD ["kalla-server"]
