# GEMINI.md - nes/coordinator

## Project Overview

This project is a stateful coordinator for a distributed stream processing system called "NES". The coordinator is responsible for managing the metadata of the system, including sources, sinks, queries, and workers. It acts as a middleware between clients and workers, providing a consistent view of the system's state.

The coordinator is designed to be fault-tolerant and uses a relational data model with an embedded SQLite database for persistence. Communication with worker nodes is done via gRPC.

The project is in its early stages, with a focus on building a minimal viable product (MVP) and then iterating on it to add more features like dynamic cluster membership, reoptimization, and a standalone network service.

## Technologies

*   **Language:** Rust
*   **Networking:** gRPC (`tonic`)
*   **Database:** SQLite (`sqlx`)
*   **Testing:** Deterministic simulation with `madsim` and property-based testing with `quickcheck`.

## Building and Running

### Building the project

To build the project, run the following command:

```bash
cargo build
```

### Running the tests

The project uses deterministic simulation testing, which is a powerful way to test distributed systems. To run the tests, use the following command:

```bash
cargo test
```

## Development Conventions

*   **Asynchronous Programming:** The project heavily relies on asynchronous programming using `tokio`.
*   **Error Handling:** The project uses the `thiserror` crate for error handling.
*   **Protocol Buffers:** The gRPC services and messages are defined using Protocol Buffers.
*   **Modularity:** The codebase is organized into modules with clear responsibilities (e.g., `catalog`, `network_service`, `coordinator`).
*   **Testing:** The project emphasizes a strong testing culture, using deterministic simulation to find and fix bugs in a reproducible way.
