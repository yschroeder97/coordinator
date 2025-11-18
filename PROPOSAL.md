# Coordinator

## Role and Responsibilities

- Cluster Membership
    - Maintain gRPC connections to workers
    - Detect unreachable workers
    - Reconnect to unreachable/failed workers
    - Update topology on membership changes
- Catalog
    - Sources
    - Sinks
    - Queries
    - Workers
-

## Status Quo
At the moment of writing, the system is accessible via three entry points:
1. The **system-level test (SLT)** tool. Used for end-to-end testing, it loads a set of test files, parses them, and submits a set of queries to an embedded/remote NES worker. It maintains its own logic for binding, tracking progress/status, and checking query results.
2. **Nebuli/YAML**. Oneshot binary target that takes a stateless approach.
   Takes command line arguments with query manipulation requests (register/start/stop/unregister/status) and either a YAML configuration (register) or a query id (start/stop/unregister), matching the interface of the current GrpcQueryManager.
   It only supports an API for queries, with source/sink descriptions contained in the query register request. The approach taken here shifts the responsibility for managing the query's progress to the user.
3. **Statement API/REPL**. Introduces SQL-like statements, including creating/showing/dropping queries/sources/sinks. Implements basic query tracking functionality.
   Currently, workers' capacities are reset after each query, rather than operators truly consuming capacity and returning it when the query terminates.

## Problems
1. Writing a new frontend requires pulling in components such as the parser, binder, planner, and query manager.
   Furthermore, the frontend needs to invoke these components directly, duplicating logic or using them in slightly different ways
   This is error-prone and does not scale well when adding new frontends (e.g., language clients, REST APIs), because changes propagate to multiple parts of the codebase. Frontends are currently allowed to do whatever they want to plan and track queries, rather than adhering to a single interface.
2. NES runs continuous (i.e., long-running) queries over unbounded data.
   Without maintaining state, we have a hard time managing the deployment and its entities over a prolonged period.
   We can't meaningfully collect heartbeats and statistics from workers to keep the cluster together, track worker capacities when allocating workloads, or trigger reoptimization of running queries. Therefore, maintaining the state is a prerequisite for all of these follow-up tasks.
3. Our data model is not clearly defined and does not account for the relational nature of some entities.
   For example, deleting a source does not affect the queries that use it.
   In its current state, it would be unclear how introducing a persistent state in the coordinator would work.

## Scope

To quickly move from a PoC to a running coordinator, we limit the scope in the following ways:
- We start with a single centralized coordinator. Reasons for this are outlined in #1120. As a result, requests can be sent only to the coordinator, and decisions affecting other workers can't be made locally.
- The coordinator will initially not expose a network interface. As a result, the coordinator needs to be embedded into the frontends as a library.
- To leverage the existing gRPC machinery, all coordinator-worker communication will be pull-based, initiated by the coordinator via RPC. Switching to a push-based model for certain events may be beneficial in some cases (cluster membership, statistics), but it is out of scope for this initial PoC.
- Performance will not be a consideration initially. Once we have a clear picture of the kind of load we expect, we can optimize for that. We focus on reliability, correctness, and simplicity first, then tackle performance issues.
- In the NES vision, we aim to cope with both infrastructure and workload changes. Currently, only dynamic workloads are supported, so we will maintain this setup and implement dynamic cluster membership down the line.
- Query reconfiguration will not be considered because key requirements, such as state migration, statistics, or reoptimization, are not yet present in the system. As a result, fault tolerance for queries can only be achieved by restarting a query fragment or the whole query.

## Assumptions

NES is a distributed system with two types of executables:
- A single coordinator process
- n worker processes
  Those are arbitrarily placed on a set of nodes and can communicate via the network, with the following restrictions:
- The coordinator can talk to all workers via RPC (although this may not be true in real-world scenarios).
- The coordinator initiates all communication to workers, not the other way around.
- Workers can communicate as specified in a static topology definition (currently, a directed acyclic graph).
- Communication between workers is initiated from the source node of a directed edge (worker1 --> worker2).

We further assume the following properties hold in the system:
### Nodes
We assume a crash-recovery model of node behavior, meaning that worker processes or the entire node can crash at any time, though they may resume later.

### Network
We assume that communication between the coordinator <=> workers and between workers is reliable (messages delivered exactly once and not out of order).
Using a reliable transport protocol, such as TCP, within RPCs achieves this.
This does not mean that we can assume end-to-end exactly-once, in-order delivery within our application.
Different incarnations of the same connection after a crash may duplicate messages, as the networking stack does not know application semantics.

### Timing
We assume a partially synchronous timing/synchrony model, meaning:
- Nodes may pause execution (e.g., the machine is busy and other threads are scheduled to run)...
- Messages may be delayed (e.g., due to network congestion)...
  ...arbitrarily, for a finite, but unknown amount of time.

### Expected Workloads
NES runs continuous queries over unbounded data.
This means:
- The system as a whole may run for a long time.
- Queries may run for a long time.
  Additional load parameters depend on what we expect to happen in real-world scenarios and on what we want to optimize. For example:
- What do we expect to change more frequently? Workloads, data, or infrastructure?
    - If data characteristics change rapidly, we should trigger reoptimization often.
    - If workloads change often, we should optimize for response time.
    - If infrastructure changes often, we should have dynamic membership and state/query migration.
- Do we expect more ad-hoc queries that come and go, or long-running queries?
- Do we expect queries to be submitted in a single batch or continuously over time?
- Are the requests to the coordinator read-heavy (querying or displaying parts of its state) or write-heavy (creating/dropping entities)?
- How large do we expect the different entities to be? (e.g., thousands of queries, millions of sources)
  For these reasons, performance is initially out of scope, as we do not yet have a clear picture of the load characteristics or the workloads NES should be optimized for.

### Requirements

The requirements are ordered by importance (functionality, reliability, flexibility, performance).
1. Functionality: implement a basic MVP that correctly processes a set of user-friendly, easy-to-understand requests. At this point, the coordinator does not take the initiative to trigger reoptimization, etc.
2. Fault Tolerance: the system as a whole continues to function and makes progress even in the presence of faults. We should gracefully handle increased load by applying backpressure to the frontends if required. At this point, the coordinator detects and responds to faults rather than failing requests at the first sight of a problem.
3. Flexibility: implement networking facilities so the coordinator can be deployed independently within the cluster and, for example, be embedded on worker nodes. This serves as a prerequisite for it to be shared by different frontends and clients.
4. Performance: implement mechanisms for deduplication, compaction, batching, merging, parallel RPCs, etc.

## Data Model

The data model of the coordinator revolves around maintaining the key metadata entities
- Workers
    - Network links to their peers
- Queries
    - Sources
    - Sinks
    - Operators
      ...and the derived representations we obtain from them.

At a high level, the coordinator's job is to maintain a network of interconnected views, known at startup, and incrementally updated as messages flow into the system.
Those changes may either originate from external clients as requests:
- UI
- REST server
- Systests
- REPL
- Language clients

Or from internal components, such as workers, as events:
- Statistics from workers about throughput, network round-trip time to peers, and task queue backlog size.
- Heartbeat messages

| Message Source | Requires Response | Requires Reliable Transmission | Frequency                  | Volume      | Example                                    |
|----------------|-------------------|--------------------------------|----------------------------|-------------|--------------------------------------------|
| External       | Yes               | Yes                            | fluctuating, unpredictable | low/medium  | Client requests (CreateQuery, DropSource)  |
| Internal       | Mostly not        | Mostly not                     | constant, predictable      | medium/high | Statistics, Heartbeats                     |

We start with a fresh state and incrementally update it as requests and events flow into the system.

These dataflows maintain different higher-level views (derived representations) that are continuously updated via incoming events and requests.

Pushing data into the base entities, arranging them by primary keys and secondary indexes, advancing time in lock step.
Me might have derived requests that are pushed into the dataflow during computation.
Implementing feedback/reconciliation loop


### Should we use persistence and/or an external data storage system?
Advantages:
- Durability of the core coordinator state is a prerequisite for fault tolerance, which is a prerequisite for using NebulaStream in production at some point.
    - When the coordinator or node crashes, it can recover from the durable state. Workers do not have all the information needed to restore a consistent metadata snapshot, and they might also be down at the coordinator restart.
- Using an external system provides querying capabilities and consistency guarantees, relieving us of the need to implement these ourselves.
- We can model invariants and constraints directly in the data model rather than in application logic.
    - Uniqueness issues (creating a source that already exists)
    - Foreign key violations (creating a query fragment and placing it on a worker that does not exist)
      Disadvantages:
- Complexity and dependency management.
- Locking us into a specific data model and even a particular system reduces flexibility.
- We cannot model all our invariants or all our views on the metadata using only the query language provided by the external system. This can lead to an awkward mix of system-specific client code and application (coordinator) code.
  Proposal:
  Add durability in stage 2 (fault tolerance), as it is not required for an initial MVP.
  Use a storage system for metadata durability in the coordinator, but prefer embeddable solutions (in-process, such as RocksDB or SQLite) that do not require maintaining a second or third system beyond our own.

### Relational Model
**Advantages**:
- Metadata entities are interrelated, meaning joins do not need to be implemented in application logic:
    - Sinks are placed on a worker
    - Query fragments (local queries, created by QueryDecomposer ) refer to the query they are part of and the worker they are currently assigned to
    - Physical sources are placed on a worker and refer to a logical source
    - Queries have at least one physical source, exactly one sink, and are composed of at least one fragment
    - Network links connect two nodes
- Support of transactions, checks, constraints, triggers, stored procedures, materialized views, etc.

**Disadvantages**:
- We need awkward ORM mapping between database objects and the coordinator's own in-memory representation.
- We can't express all computations in terms of database queries; we'll likely have derived collections in memory (like the topology and query graph), which means the in-memory representations and the database must be kept in sync and up to date at all times, requiring the mapping layer mentioned above.
- More complex maintenance and configuration (compared to a key-value store).

### Key-Value Model
**Advantages**:
- Simple, easy to reason about.
- Often: high write throughput.
- Easily applicable to our use case, all entities have (named) ids that are suitable as keys.
- We can use the event-sourcing model, where we store client commands/requests rather than the actual data. This allows us to store each entity and its derived representations in our preferred format in memory while providing durability when combined with checkpointing consistent snapshots from time to time.
    - This can later be extended nicely to use the Raft consensus algorithm, making the coordinator more robust and increasing availability.

**Disadvantages**:
- Does not support complex queries involving joins/aggregations.
- Does not have the expressive constraint checking, triggers, or materialized views that relational engines have.
- Limited support for transactions.

### Graph Model
**Advantages**:
- Topologies and query plans are both a good fit.
    -  Advanced querying capabilities like traversing, finding paths, etc.
- Our metadata could be modeled as a single, large graph of the topology, with queries/operators placed on it (a graph within a graph).

**Disadvantages**:


**Proposal**:
We should avoid distributing data across multiple models managed by separate systems.
Each new external system brings an additional layer of maintenance and configuration, increasing complexity.
Furthermore, we build large blocks of client code to handle communication with these systems, locking us in.

- Should we return the create query request with a pending state, forcing the client to check the query status?
- Where do we implement schema, descriptors, etc?

## User-facing Messages

### Should we allow clients (frontends, users) to query our metadata arbitrarily?
Database systems sometimes allow querying internal metadata, such as tables, views, and indexes.
We could also expose our metadata via SQL or our query dialect.

**Advantages**:
- Flexibility allows clients to do complex analytics based on the use case.
- Additional metadata can be used directly by clients without touching the coordinator code.

**Disadvantages**:
- Would use the query language dialect of the underlying system (e.g., SQLite), which is confusing.
- Security issues (SQL injection); restriction of what is allowed to query is complex.
- Compatibility issues when internal schema changes.
- Faulty query results are passed to the client; we have no insight into what went wrong.

**Proposal**:
We should probably not allow external users and systems to write arbitrary SQL queries against our internal metadata.
Instead, we carefully expose a set of requests that can be extended when required.

### Should we allow updates?
**Advantages**:
- Clients may want to update a query to use a different source/sink, update the configuration of a particular source, update the port of a worker, etc.

**Disadvantages**:
- Updates can have dangerous side effects or prerequisites (e.g., the worker must be restarted with the new port) that are hard to check and handle.
- Almost always create a chain of follow-up tasks or checks.

**Proposal**:
We should only support CREATE/DROP requests against the coordinator.
If the client made a mistake, a correction (drop and a new create) request must be submitted.

### Should deletes cascade/propagate automatically?
On delete, references from other entities may be invalid.
For example, when a logical source is dropped, queries that use it may fail.

**Advantages**:
- Automatic propagation of deletes without further manual interference

**Disadvantages**:
- Clients may not know or intend the consequences of a cascading delete

**Proposal**:
Handle it like Docker: return an error, or possibly a list of entities that use/refer to the thing to be dropped.
The client must first drop all referring entities and try again.
Later, we can implement optional cascading deletes.

| Request Name           | Target Entity    | Type   | Arguments                                                                                   | Response                                                                 | Network Involved | Possible Errors                                                                                                                                                  |
|------------------------|------------------|--------|---------------------------------------------------------------------------------------------|--------------------------------------------------------------------------|------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `GetLogicalSource`     | Logical Sources  | GET    | `by_name` (optional)                                                                        | `Result<LogicalSource, Err>`                                             | No               | `DoesNotExist`                                                                                                                                                   |
| `GetPhysicalSource`    | Physical Sources | GET    | `for_logical` (optional)<br>`on_node` (optional)<br>`by_type` (optional)                    | `Result<Vec<PhysicalSource>, Err>`                                       | No               | `DoesNotExist`                                                                                                                                                   |
| `GetSink`              | Sinks            | GET    | `by_name` (optional)<br>`on_node` (optional)<br>`by_type` (optional)                        | `Result<Vec<Sink>, Err>`                                                 | No               | `DoesNotExist`                                                                                                                                                   |
| `GetQuery`             | Queries          | GET    | `by_id` (optional)<br>`with_state` (optional)<br>`on_worker` (optional)                     | `Result<Vec<Query>, Err>`                                                | Yes              | `DoesNotExist`<br>`NetworkError`                                                                                                                                 |
| `DropLogicalSource`    | Logical Sources  | DROP   | `name`                                                                                      | `Result<Option<LogicalSource>, Err>`<br>(Some if exists, None otherwise) | No               | `ReferencedPhysicalSourceExists`                                                                                                                                 |
| `DropPhysicalSource`   | Physical Sources | DROP   | `id`                                                                                        | `Result<Option<PhysicalSource>, Err>`                                    | No               | `ReferencedQueryExists`                                                                                                                                          |
| `DropSink`             | Sinks            | DROP   | `name`                                                                                      | `Result<Option<Sink>, Err>`                                              | No               | `ReferencedQueryExists`                                                                                                                                          |
| `DropQuery`            | Queries          | DROP   | `id`                                                                                        | `Result<Option<Query>, Err>`                                             | Yes              | `NetworkError`                                                                                                                                                   |
| `CreateLogicalSource`  | Logical Sources  | CREATE | `name`<br>`schema`                                                                          | `Result<(), Err>`                                                        | No               | `AlreadyExists`<br>`EmptySchema`<br>`InvalidSchema`                                                                                                              |
| `CreatePhysicalSource` | Physical Sources | CREATE | `logical_source_name`<br>`placement`<br>`source_type`<br>`source_config`<br>`parser_config` | `Result<PhysicalSourceId, Err>`                                          | No               | `AlreadyExists` (how to check that?)<br>`WorkerDoesNotExistForPhysical`<br>`LogicalSourceDoesNotExistForPhysical`<br>`SourceTypeDoesNotExist`<br>`InvalidConfig` |
| `CreateSink`           | Sinks            | CREATE | `name`<br>`schema`<br>`placement`<br>`sink_type`<br>`config`                                | `Result<(), Err>`                                                        | No               | `AlreadyExists`<br>`WorkerDoesNotExistForSink`<br>`SinkTypeDoesNotExist`<br>`InvalidConfig`                                                                      |
| `CreateQuery`          | Queries          | CREATE | `name` (optional)<br>`statement`<br>`sink`                                                  | `Result<Option<QueryId>, Err>`                                           | Yes              | `ParserError`<br>`NetworkError`<br>`SinkDoesNotExistForQuery`<br>`BinderError`<br>`OptimizerError`<br>`PlacementError`                                           |
| `CreateWorkers`        | Workers          | CREATE | `host_name`<br>`grpc_port`<br>`data_port`<br>`num_slots`<br>`peers`                         | `Result<(), Err>`                                                        | Yes              | `NetworkError`                                                                                                                                                   |

## Fault Tolerance

Fault type
Detection (from coordinators' POV)
Handling (from coordinators' POV)
Open Questions
Worker process crashes
RPC to worker fails
1. Move the worker to unreachable  state
2. Try to reconnect with a user-configurable strategy (e.g., exponential backoff, 3 times).
3. On success, redeploy query fragments according to their last observed state (registered, started)
4. On failure (after too many failed attempts), declare the worker dead and remove it from the cluster.
   In case of failure, removing the worker has additional consequences:
- Requests that arrived in the meantime and are related to the worker (even indirectly, like the creation of a source that is placed on the worker) should be responded to with an error; derived RPCs need to be cancelled
- Related entities need to be deleted:
- Network links
- Physical sources
- Query fragments and queries
- Contact remaining workers and stop the query fragments related to the queries that were deployed on the failed worker.
  What happens to requests that arrive during the retry that are in some way related to the worker?
-
- Option 1: defer them until the issue is sorted out.
- Option 2: respond failure, let the sender retry later

What happens to in-flight RPC requests that are currently waiting for a response?
[TODO]

What if declaring the worker dead was a false positive (the network was interrupted) and the worker is still running?
When reconnecting within the retry period, the resend must be idempotent (e.g., ignore a start query request if the query is already running). [Need to look into gRPC to see if the protocol does this, or if the application logic needs to do this]. If reconnecting fails, it does not matter to the coordinator.

What are peer workers doing in the meantime?
Currently, workers try to reconnect endlessly with their peers. This fits into the model of the coordinator resolving the issue. Downstream workers' query fragments are idle in the meantime, and upstream workers' queries apply backpressure on their sources. If the source system or protocol does not allow propagating backpressure, data will be lost (e.g., packet loss in UDP).
Coordinator process crashes
Needs to be detected by the user or the frontend
1. Restart coordinator
2. Load the latest snapshot from the database and rebuild the in-memory state.
3. Continue processing messages.
   What about the messages whose effects have not been applied/partially applied/not persisted?
   Worker <=> worker network fault
   It can't be detected currently
   /* Nothing */
   The workers will attempt to re-establish the connection until it is successful; the coordinator plays no role in this process.
   A potential solution is to migrate parts of the query to other parts of the topology, reconfigure the network source/sink, or use another path through the topology for sending intermediate results. Currently, we have no logic on the worker side to implement this.
   Coordinator => worker network fault
   RPC to worker fails
   Can't be distinguished from a worker crashing from the POV of the coordinator.
   Therefore, apply the same strategy, ensuring the worker is idempotent with respect to updates it already has.


## Control Flow and Consistency Model

- We should use a consistent framework and programming model to update a deployment's state.
1. Serialize requests on a single thread
2. Batch up requests (may lead to anomalies like cancelled updates)
3. Introduce transactions
- Data from the same source (client) is guaranteed to be processed in order, allowing interleavings between different clients.

### Write Skew
![Write Skew Race Condition](/home/yannik/Downloads/write_skew.png)


## Invariants

| Invariant/Property                                                      | Request                                   | Type        | Detection                                                                                     | Reaction                                                              |
|-------------------------------------------------------------------------|-------------------------------------------|-------------|-----------------------------------------------------------------------------------------------|-----------------------------------------------------------------------|
| Logical source `logical_source_name` unique                             | `CreateLogicalSource`                     | Uniqueness  | DB engine/check before insert                                                                 | Fail to process request                                               |
| Logical source `Schema` non-empty, data types exist, field names valid? | `CreateLogicalSource`                     | Logic       | Check before request construction, on deserialization into a native type, or before insertion | Fail to process/construct request                                     |
| Physical source unique?                                                 | `CreatePhysicalSource`                    | Uniqueness  | DB engine/check before insert (Eq --> all fields equal)                                       | Fail to process request                                               |
| Physical source `worker` referenced exists                              | `CreatePhysicalSource`                    | Foreign Key | DB engine/check before insert                                                                 | Fail to process request                                               |
| Physical source `logical` source references exists                      | `CreatePhysicalSource`                    | Foreign Key | DB engine/check before insert                                                                 | Fail to process request                                               |
| Physical source `source_type` referenced exists                         | `CreatePhysicalSource`                    | Foreign Key | DB engine/check before insert                                                                 | Fail to process request                                               |
| Physical source `source_config` and `parser_config` valid               | `CreatePhysicalSource`                    | Logic       | Validate descriptor                                                                           | Fail to process request                                               |
| Sink `worker` referenced exists                                         | `CreateSink`                              | Foreign Key | DB engine/check before insert                                                                 | Fail to process request                                               |
| Sink `sink_type` exists                                                 | `CreateSink`                              | Foreign Key | DB engine/check before insert                                                                 | Fail to process request                                               |
| Sink `name` unique                                                      | `CreateSink`                              | Uniqueness  | DB engine, check before insert                                                                | Fail to process request                                               |
| Sink `config` valid for sink type                                       | `CreateSink`                              | Logic       | Validate descriptor against sink type                                                         | Fail to process request                                               |
| Query `statement` parseable                                             | `CreateQuery`                             | Logic       | Run parser on statement                                                                       | Fail to process request (ParserError)                                 |
| Query AST bindable (i.e., sources/sink/etc exist)                       | `CreateQuery`                             | Logic       | Run binder on parsed AST                                                                      | Fail to process request (BinderError)                                 |
| Query optimizable (valid query plan can be generated)                   | `CreateQuery`                             | Logic       | Run optimizer on bound statement                                                              | Fail to process request (OptimizerError)                              |
| Exists a network path from sources to sink with sufficient capacity     | `CreateQuery`                             | Logic       | Run placer on optimized query plan                                                            | Fail to process request (PlacementError)                              |
| Query running on either all or no workers                               | `CreateQuery`/`DropQuery`                 | Network     | Submit/remove query to/from all participating workers                                         | Retry x times, then spawn cleanup task until desired state is reached |
| Capacity on each worker >= 0                                            | `CreateWorkers`/`CreateQuery`/`DropQuery` | Logic       | Check no queries use this sink                                                                | Fail to process request, show list of queries that use the sink       |
| Worker `host_name` and ports valid                                      | `CreateWorkers`                           | Logic       | Check address format and port ranges                                                          | Fail to construct/process request                                     |
| Worker `peers` referenced exist                                         | `CreateWorkers`                           | Foreign Key | Check after all workers have                                                                  | Fail to process request                                               |
| Topology has no cycles                                                  | `CreateWorkers`                           | Logic       | Run validation after topology construction                                                    | Fail to process request                                               |
| Topology has at least one src (in-degree 0) and one snk (out-degree 0)  | `CreateWorkers`                           | Logic       | Run validation after topology construction                                                    | Fail to process request                                               |
| Every src/snk is reachable by at least one src/snk                      | `CreateWorkers`                           | Logic       | Run validation after topology construction                                                    | Fail to process request                                               |
| Every worker reachable from the coordinator                             | `CreateWorkers`                           | Network     | Establish RPC connection to worker at startup of `ClusterService`                             | Retry x times, then fail to start coordinator (NetworkError)          |
| Logical source not referenced by physical sources                       | `DropLogicalSource`                       | Foreign Key | Check no physical sources reference this logical source                                       | Fail to process request, show list of physical sources that violate   |
| Physical source not referenced by queries                               | `DropPhysicalSource`                      | Foreign Key | Check no query fragments use this physical source                                             | Fail to process request, show list of queries that use the source     |
| Sink not referenced by queries                                          | `DropSink`                                | Foreign Key | Check no queries use this sink                                                                | Fail to process request, show list of queries that use the sink       |

### Open Questions
- Do we want to impose constraints on the names for queries/sources/sinks?
- How do we handle partially created/started queries?

Concurrency Issues
- Two or more CreateQuery/DropQuery Requests (placement conflicts)
- CreateWorker/DropWorker request

Idea: incorporate responsiveness into placement decisions

## Testing

As we aim to scale NES to large deployments, we need to face situations with many uncertainties, such as messages being lost or delayed, or nodes or processes crashing.
Moreover, other parts of the system can't reliably know about the cause of the problems.
The number of possible states of such a large system is exploding.
Therefore, it is hard to enumerate all possible error scenarios beforehand via formal verification and make the system correct in the first place.
On the other hand, techniques like chaos testing can validate system behavior, but can't guarantee the reproducibility of error cases.

### Unit Testing
Advantages:
- Easy to set up and run.
- Fast.

Disadvantages:
- Most correctness and reliability issues stem from interactions among modules/components.
- Can't test error scenarios that include multiple nodes.

### Formal Verification
Advantages:
- Provides mathematical guarantees of correctness for specified properties
- Can prove the absence of certain classes of bugs
- Catches subtle edge cases that are hard to find through testing
- Design flaws are discovered early in the development process

Disadvantages:
- Requires significant expertise in formal methods and specialized tools (TLA+, Coq, etc.)
- Time-consuming and expensive to create and maintain formal specifications
- Difficult to model all aspects of a real system (I/O, performance characteristics, timing)
- Gap between formal model and actual implementation may introduce bugs

### Chaos Testing
Advantages:
- Tests system behavior under realistic failure conditions
- Validates fault tolerance mechanisms in production or production-like environments
- Builds confidence in system resilience
- Discovers unexpected failure modes and interactions

Disadvantages:
- Non-deterministic: failures are hard to reproduce
- Requires production-like infrastructure, which can be expensive
- Difficult to achieve comprehensive coverage of failure scenarios
- Results are probabilistic, not exhaustive

### Deterministic Simulation Testing & Property Testing
DST aims to deterministically test a system without requiring an actual distributed system with multiple processes or even multiple physical machines.
It achieves this by removing all factors of uncertainty (multithreaded scheduler, random numbers, timing) by mocking the components responsible for them.
We can combine DST with property-based testing, where we specify invariants/properties instead of manually designing test cases.
The test system will then create inputs and test our invariants, while the DST system simulates an error-prone distributed system.
On a failure, the property tester will try to explore the state space and find an easier input to reproduce the failure.
The used seed will help us manually reproduce the error and fix the bug.

Advantages:
- Reproducible: same seed produces same execution sequence
- Fast: can test thousands of scenarios without real network delays
- Comprehensive: can explore many interleavings and failure scenarios systematically
- Combines benefits of unit tests (speed, reproducibility) with integration testing (realistic scenarios)
- Property-based testing to automatically create test cases
- Can use techniques like state-space exploration to increase coverage

Disadvantages:
- Requires careful simulation framework design and maintenance
- Simulated environment may not perfectly match production behavior
- Time budget limits exhaustive exploration of large state spaces
- Developers must write meaningful properties to check
- Initial setup cost is higher than simple unit tests

## Other Systems' Approaches

### Legacy NES (Coordinator)


### Flink (JobManager/Dispatcher)


### Kafka (Controller/KRaft)


### Spark Streaming


### Materialize (adaptord)


### RisingWave (meta)


## Implementation Plan

1. Implement an MVP that implements a coordinator lib with the following properties:
    1. A single-threaded event loop that serializes the minimal set of requests mentioned.
    2. Will not support:
        1. Transactions
        2. Statistics
        3. Heartbeats
        4. Infrastructure changes
        5. Reoptimization passes
        6. Network service interface for frontends or workers
    3. Fail every request that would cause an invalid state or an invariant to be broken.
    4. Have a test suite comprising property testing using a deterministic simulation framework.
2. Iterate from there by:
    1. Gradually turning unrecoverable errors into recoverable ones by implementing fault tolerance mechanisms (e.g., restarting query fragments on resumed workers, reconnecting to unreachable workers).
    2. Extending the set of supported requests, making existing requests more powerful.
    3. Introducing transactions.
    4. Introducing dynamic cluster membership.
    5. Introducing network endpoints for the coordinator so that it can be deployed as a standalone process or embedded into workers.
    6. Triggering reoptimization passes using statistics.

## Further Reading
- [Deterministic Simulation Testing 1](https://risingwave.com/blog/deterministic-simulation-a-new-era-of-distributed-system-testing/)
- [Deterministic Simulation Testing 2](https://risingwave.com/blog/applying-deterministic-simulation-the-risingwave-story-part-2-of-2/)
- [MadSim](https://github.com/madsim-rs/madsim)
