SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

COMPOSE_FILE := infra/compose/docker-compose.yml

.PHONY: help
help: ## Show available targets
	@awk 'BEGIN{FS=":.*##"; printf "Targets:\n"} /^[a-zA-Z0-9_.-]+:.*##/{printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# ---------- Rust ----------

.PHONY: build
build: ## cargo build --workspace
	cargo build --workspace

.PHONY: release
release: ## cargo build --workspace --release
	cargo build --workspace --release

.PHONY: test
test: ## cargo test --workspace
	cargo test --workspace

.PHONY: check
check: ## cargo check --all-targets
	cargo check --workspace --all-targets

.PHONY: clippy
clippy: ## cargo clippy with warnings as errors
	cargo clippy --workspace --all-targets -- -D warnings

.PHONY: fmt
fmt: ## cargo fmt
	cargo fmt --all

.PHONY: fmt-check
fmt-check: ## cargo fmt --check
	cargo fmt --all -- --check

.PHONY: clean
clean: ## cargo clean
	cargo clean

# ---------- Local infra ----------

.PHONY: up
up: ## Start local Postgres/Redis/ClickHouse/NATS
	docker compose -f $(COMPOSE_FILE) up -d

.PHONY: down
down: ## Stop local infra
	docker compose -f $(COMPOSE_FILE) down

.PHONY: nuke
nuke: ## Stop local infra and DELETE all volumes (destructive)
	docker compose -f $(COMPOSE_FILE) down -v

.PHONY: ps
ps: ## Show local infra status
	docker compose -f $(COMPOSE_FILE) ps

.PHONY: logs
logs: ## Tail local infra logs
	docker compose -f $(COMPOSE_FILE) logs -f --tail=100

# ---------- Schema migrations ----------

TS_ENV ?= dev

.PHONY: migrate
migrate: ## Apply pending Postgres + ClickHouse migrations (TS_ENV=dev)
	cargo run -q -p ts-storage --bin ts-migrate -- --env $(TS_ENV) up

.PHONY: migrate-status
migrate-status: ## Print applied migrations on both backends
	cargo run -q -p ts-storage --bin ts-migrate -- --env $(TS_ENV) status

.PHONY: migrate-validate
migrate-validate: ## Parse on-disk migrations and print checksums (no network)
	cargo run -q -p ts-storage --bin ts-migrate -- --env $(TS_ENV) validate

# ---------- Quality gate ----------

.PHONY: ci
ci: fmt-check clippy test ## What CI runs
