FROM rust:1.89-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release

FROM debian:bookworm-slim
LABEL org.opencontainers.image.title="personal-agent-push-gateway" \
      org.opencontainers.image.version="2026.8.29"
RUN apt-get update && apt-get install --no-install-recommends -y ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 gateway \
    && useradd --system --uid 10001 --gid 10001 --no-create-home gateway
COPY --from=builder /build/target/release/push-gateway /usr/local/bin/push-gateway
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/push-gateway"]
