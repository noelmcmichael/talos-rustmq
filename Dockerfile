# Build stage - compile Rust binary
# Explicitly target AMD64 for AWS EC2 deployment
FROM --platform=linux/amd64 rust:1.91-slim as builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# Build release binary
RUN cargo build --release

# Runtime stage - minimal image
FROM --platform=linux/amd64 debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy binary from builder
COPY --from=builder /build/target/release/rustmq-server /usr/local/bin/rustmq-server

# Create data directory
RUN mkdir -p /data/rustmq

# Configuration
ENV RUSTMQ_DATA_DIR=/data/rustmq
ENV RUSTMQ_PARTITIONS=12
ENV RUSTMQ_PRODUCER_PORT=9092
ENV RUSTMQ_CONSUMER_PORT=9093
ENV RUSTMQ_METRICS_PORT=9094

# Expose ports
EXPOSE 9092 9093 9094

# Health check
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
  CMD curl -f http://localhost:9094/metrics || exit 1

# Run as non-root
RUN useradd -m -u 1000 rustmq && \
    chown -R rustmq:rustmq /data/rustmq
USER rustmq

CMD ["/usr/local/bin/rustmq-server"]
