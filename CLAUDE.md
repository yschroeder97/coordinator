# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

NES Coordinator is a Rust-based stateful coordinator for the NebulaStream distributed stream processing system. It serves as the control plane, managing metadata (workers, queries, sources, sinks) via SQLite and orchestrating query execution across worker nodes via gRPC.

## Rules

- **Test timeout:** Never run any test invocation command (cargo test, cargo nextest, etc.) without a Bash tool timeout of less than 1 minute.
- **Test frequency:** Do not run tests after every small change. Tests take ~50s. Run them periodically after larger batches of changes, or when specifically asked.

## Build and Test Commands

```bash
# Build
cargo build

# Run all tests (unit + integration, excludes simulation)
cargo test --features testing

# Run deterministic simulation tests only (with madsim)
RUSTFLAGS="--cfg madsim" cargo test -p coordinator --features testing --test '*'

# Run a specific test
cargo test --features testing test_name

# Lint and format
cargo clippy
cargo fmt
```

**Environment:** Set `DATABASE_URL` (e.g., `sqlite:test.db` or `sqlite::memory:`) in `.env` or environment.

## Architecture

### Crates

- **model**: Sea-ORM entity definitions (Query, Worker, LogicalSource, PhysicalSource, Sink, Fragment) and request/response types
- **catalog**: SQLite-backed metadata storage with `Reconcilable` trait for controller integration
- **controller**: Asynchronous controllers that reconcile desired state with actual state
- **coordinator**: Entry point, request dispatching, simulation test harness
- **migration**: Sea-ORM database migrations

### Core Pattern: Reconciliation-Based Control Flow

SQLite is the source of truth. Clients write desired state to the Catalog, controllers observe changes via `NotificationChannel` (bidirectional `watch` channels), and reconcile toward desired state via worker RPCs. The `Reconcilable` trait exposes `subscribe_intent()`, `subscribe_state()`, and `get_mismatch()` for this loop. This enables self-healing: if messages are lost or components crash, the next reconciliation loop corrects state.

### Key Components

```
Coordinator (coordinator/)
├── Request Handler: dispatch!/dispatch_blocking! macros route to catalog
├── Catalog (catalog/)
│   ├── QueryCatalog: Query lifecycle (Pending→Planned→Registered→Running)
│   ├── WorkerCatalog: Worker registration and topology
│   ├── SourceCatalog: Logical and physical sources
│   └── SinkCatalog: Output sinks
└── Controllers (controller/)
    ├── QueryService: Watches query intent, spawns QueryReconciler per query
    ├── QueryReconciler: State machine (Pending→Planned→Registered→Running) with rollback
    ├── ClusterService: Watches worker intent, reconciles worker connections
    ├── HealthMonitor: Periodic gRPC health checks, marks workers Unreachable
    └── WorkerClient: gRPC calls to workers with retry
```

### Database Schema

Single migration in `migration/src/`. Tables: `workers`, `network_links`, `logical_sources`, `physical_sources`, `sinks`, `query`, `fragment`. State machines enforced via triggers and check constraints.

### gRPC Protocol

Defined in `proto/SingleNodeWorkerRPCService.proto` and `proto/health.proto`. Auto-compiled by `controller/build.rs` using tonic-build.

## Key Patterns

**Request/Response Envelope:** `Request<P, R>` wraps a payload with a oneshot reply channel. `into_request!` macro (in `controller/src/request.rs`) generates `From` impls for `CoordinatorRequest` variants. `dispatch!` and `dispatch_blocking!` macros route requests to catalog methods.

**Error Handling:** Domain-specific error types per catalog module using `thiserror`, with `#[from]` conversions bubbling up to `CatalogErr`.

**Testing:**
- Property-based unit tests: `proptest` with `Generate` trait for type-tied strategies
- Deterministic simulation: `#[madsim::test]` with `cargo nextest` for process-per-test isolation
- Failpoint injection: `fail` crate for crash recovery testing

**Concurrency:** Tokio runtime with `watch` channels for notifications, `JoinSet` for tracking concurrent tasks, `tokio::select!` for multiplexing.

## Adding New Features

**New Request Type:**
1. Define model types (Create/Get/Drop) in `model/src/`
2. Add catalog method in the appropriate `catalog/src/*_catalog.rs`
3. Add variant to `CoordinatorRequest` enum in `coordinator/src/coordinator.rs`
4. Implement `into_request!` conversion for the new variant
5. Add `dispatch!` or `dispatch_blocking!` rule in `coordinator/src/request_handler.rs`

**Schema Changes:**
1. Add migration in `migration/src/`
2. Update sea-orm entities in `model/src/`
3. Run `cargo test` to validate

**gRPC Changes:**
1. Modify `proto/SingleNodeWorkerRPCService.proto`
2. Run `cargo build` (auto-rebuilds via `controller/build.rs`)
3. Update `WorkerClient` in `controller/src/cluster/worker_client.rs`

## Code Style

- **No comments:** Do not add comments (inline, doc comments, or block comments) to code. Comments are added manually later.
- **No magic numbers:** Never use literal numeric values inline. Always extract them into named constants.
- **Imports:** Only use fully qualified paths (e.g., `std::collections::HashMap`) when there is a name collision or the type name is heavily overloaded across crates (e.g., `flume::Sender` vs `tokio::sync::mpsc::Sender` vs `oneshot::Sender`). Otherwise, import and use the type name directly.

## Git Workflow

- **Always rebase, never merge.** Use `git rebase` to integrate changes from other branches. Do not create merge commits.

## Key Dependencies

- **madsim**: Deterministic simulation framework (patches tokio, tonic)
- **sea-orm**: ORM for SQLite-backed catalog (pinned to 1.1.1 for madsim compatibility)
- **tonic/prost**: gRPC framework and protobuf
- **proptest**: Property-based testing with `Generate` trait for type-tied strategies
- **fail**: Failpoint injection for crash recovery testing
