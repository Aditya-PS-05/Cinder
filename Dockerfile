# syntax=docker/dockerfile:1.7

# --- builder -------------------------------------------------------------
# Pin rustc to a recent stable; the workspace MSRV is 1.75 (Cargo.toml).
FROM rust:1.82-bookworm AS builder

WORKDIR /src

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        pkg-config \
        ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY . .

# The cache mounts keep the registry and target dir warm across CI
# invocations. `--locked` refuses to touch Cargo.lock, so an out-of-date
# lockfile fails the build rather than silently being rewritten.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --workspace --locked \
 && mkdir -p /out \
 && for bin in ts-live-run ts-paper-run ts-tape ts-md-binance \
               ts-report ts-backtest ts-migrate; do \
        cp "target/release/${bin}" /out/; \
    done

# --- runtime -------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        ca-certificates \
        tini \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system ts \
 && useradd  --system --gid ts --uid 10001 --home /app --shell /sbin/nologin ts

WORKDIR /app

COPY --from=builder /out/                         /usr/local/bin/
COPY --chown=ts:ts  config/                       /app/config/
COPY --chown=ts:ts  infra/migrations/             /app/migrations/

USER ts

# `tini` reaps zombies and forwards signals — important when the trading
# binaries run as PID 1 in Kubernetes pods.
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/usr/local/bin/ts-live-run", "--help"]
