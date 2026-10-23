# Build from the repository root:
#   docker build -f sdk-example/Dockerfile -t supra-indexer-example .
FROM rust:1.90-bookworm AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends libpq-dev && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release -p sdk-example

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends libpq5 ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 indexer
COPY --from=builder /app/target/release/sdk-example /usr/local/bin/sdk-example
USER indexer
ENV HTTP_ADDR=0.0.0.0:8080
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s CMD ["sh", "-c", "exec 3<>/dev/tcp/127.0.0.1/8080"]
ENTRYPOINT ["sdk-example"]
