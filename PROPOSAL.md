Reintroduce a stateful, continuously running coordinator back into NES.
This proposal describes a two-phase periodic, snapshot-based approach that packages the duties of managing an NES deployment into a single library.
Some ideas are derived from the legacy coordinator, where @ankitgit invested a significant amount of time, incorporating my ideas for improvements and engaging in discussions.

# 1. Status Quo
At the moment of writing, the system is accessible via three entry points:
1. The **systest tool**. Used for testing, it loads a set of test files, parses its custom DSL, and submits a set of queries to an embedded/remote NES worker. It maintains its own logic of binding, tracking progress/status, and checking the results of queries.
2. **NebulI/YAML**. Oneshot binary target that takes a stateless approach.
   It takes command line arguments involving query requests (register/start/stop/unregister) and either a YAML configuration (register) or a query ID (start/stop/unregister), matching the interface of the current `GrpcQueryManager`.
   It only supports an API related to queries, with descriptions of sources/sinks being contained in the register request of a query. The approach taken here shifts the responsibility for managing the query's progress to the user.
4. **Statement API/REPL**. Introduces SQL-like statements, including creating/showing/dropping queries/sources/sinks. Implements very basic query tracking functionality.

The general deployment process can be summarized as follows:
- One or more (distributed) SingleNodeWorkers that run in standalone processes OR embedded in one of the frontends.
- One or more frontends (Nebuli/REPL/systest).
- No infrastructure changes allowed, only query changes.

# 2. Problems
1. Frontends are **tightly intertwined with other tasks** like binding, planning, submitting, and tracking the state of queries.
   These components are used in slightly different ways, scattered around the mentioned binary targets in NES.
   This is error-prone and makes maintaining the system harder, because **changes propagate** to different locations in the codebase.
2. We have no designated approach for **concurrently managing events that reach the system**.
   In the current state of the different frontends, this might not be a problem.
   Still, as soon as requests are received from multiple threads or internal messages are interleaved with external ones, we need a more thoughtful approach to concurrency.
   Neither letting frontend threads carelessly invoke the other query planning components nor slapping locks into every shared resource is a good approach.
3. NES runs continuous queries over unbounded data. Without maintaining state, we have a hard time managing the deployment and all its entities over a prolonged period of time.
   We can not meaningfully collect heartbeats and statistics from workers to keep the cluster together, keep track of worker capacities when allocating workloads to them, or trigger reoptimization of running queries. Therefore, **maintaining state is a prerequisite** for all of these follow-up tasks.
4. Our data model is not defined clearly, and does not take the relational nature of some of the entities into account. We have catalogs that do not encode these relations, nor do they currently support cascading deletes. In the current state, it would be unclear how introducing a persistent state in the coordinator could work.

# 3. Goals
1. **Separation of Concerns**: afterwards, none of the frontends should need to implement any logic regarding parsing the query string, binding, interacting directly with the catalogs, interacting with the worker, tracking status and progress of queries, etc.
   They simply formulate requests and forward them to the coordinator (addresses P1).
   Implementing new frontends should be way easier after this change.
   Similarly, the design of the coordinator should not restrict the degrees of freedom in the optimizer/placer in any way.
3. **Thread-safety**: frontends should not need to care about this and be allowed to invoke the coordinator from multiple threads safely.
   At the same time, the design should be open for extension to a diverse set of parallelization strategies in the optimizer (addresses P2).
5. **Robustness**: failures due to processing internal/external messages should not influence other messages and never lead to a crash of the entire coordinator.
   Furthermore, an overload should be handled gracefully by applying backpressure to the frontends and the workers.
   Due to the coordinator being a single point of failure, we need a robust design that prevents congestion within the coordinator.
7. **Extensibility**: even if we do not implement this right away, the design should allow for adding new (types of) messages to the coordinator and enhanced monitoring capabilities. This will facilitate research on the coordinator.
8. **Simplicity**: the coordinator should be easy to configure, with relatively few parameters
9. **Flexibility**: the coordinator should be usable from the network and within the same process, from both asynchronous runtimes and "normal" threads, to not pose any restrictions on the execution context from which it is invoked.

# 4. Non-Goals
1. The coordinator will not (initially) support an extensive set of messages, but an absolute minimum.
2. Implement any logic related to query parsing, binding, optimization, or placement.
   It only provides the respective components with a consistent snapshot of the entities within the NES deployment to work with.
4. This initial proposal, as well as the initial PoC implementation, will not target fault tolerance, persistence, or periodic reoptimization.
   We only lay out the foundation for these things to be implemented on top of.

# 5. Core Design Choices

This chapter outlines the core design decisions that the coordinator should implement, together with alternatives and arguments for why I would neglect them.

## 5.1 Centralized Coordinator
This has been a frequent point of discussion, and there is no best answer.
The overarching benefit of a centralized coordinator is that it enables achieving correctness and consistency in a reasonably simple implementation in a reasonable amount of time.
It provides a global view of the deployment state, freeing us from worrying about distributed consistency and consensus protocols.
This enables quick decisions without consulting other participants within the deployment.
As the main disadvantages of this design, these two are brought up the most:
1. **Single point of failure**: if the coordinator crashes, and if the other participants do not have autonomy, we can not proceed reliably, and the whole deployment falls apart.
2. **Limited scalability**: if the coordinator is limited to a single instance, we are constrained by the resources on the machine on which it runs.
   At some level of incoming traffic, it will fail to process more requests.

Regarding 1), a coordinator would be deployed on a cloud node or a node local to the user, thereby guaranteeing a certain level of reliability.
The most frequently occurring problem is that the process crashes, which can be mitigated by introducing persistence for the catalogs' state (e.g., via an embedded relational database).
If an even stronger safety net is required, we could use a replicated/distributed database.
For 2), a similar argument applies: a cloud node will likely be able to handle a reasonable amount of traffic that will be enough for 99% use cases, especially for real-world use cases in the coming years.
Remember that we target long-running queries instead of ad-hoc queries, which amortizes deployment over a larger time interval.
For research, contributors are free to implement their own, more exotic coordination mechanisms.
We can at least mitigate the problems of limited scalability by applying backpressure gracefully by blocking threads or via the network protocol in case of an overloaded coordinator.
That way, it can continue reliably.
We leave the coordinator open for extension as much as possible by decoupling the different architectural components.

### Alternative 1: distributed coordinator

A future improvement on the centralized approach.
Coordinator instances that serve a specific region of the deployment could be placed at suitable points in the network topology, with a router/load balancer in front that sends the appropriate messages to them.
Not suitable as a baseline because the centralized coordinator is a prerequisite for this.

### Alternative 2: decentralized/no coordinator
Proposes large/full autonomy of the worker nodes.
A large variety and number of approaches are thinkable, but an initial approach that could serve as a baseline is unclear.
Does not pose the problems of the centralized approach (no single point of failure, has infinite scalability).
However, implementing this in the current state of the system is too complex and poses a high risk in terms of implementing it robustly and correctly within a reasonable timeframe.
This does not mean that it is a bad idea to shift to a more decentralized approach with shared responsibilities gradually.
Instead, it can evolve naturally from the initial centralized approach.

**For the reasons outlined, I propose implementing a centralized, single-node coordinator that could be extended to be distributed and use persistence via an embedded database.**

## 5.2 Packaging/Deployment

I imagine two options for packaging the coordinator.
1. As a **library** intended to be used within one of the frontends (e.g., the systest tool or the REPL).
2. As a standalone **binary**, it enables serving multiple frontends at once.

The coordinator will link against all the current NebulI query planning components (global optimizer, catalogs, parser).
This enables the frontends to link against the coordinator, simplifying their build.
I suggest starting with the library implementation for use within either of the frontends, but with the future extension to a binary in mind.

## 5.3 Interface

With goals 1) and 2) in mind, I propose an entirely **message-based interface** to the coordinator.
A message-driven system enables **loose coupling** between the coordinator and its clients, and helps the implementation stay open for extension by adding more messages (types).
Moreover, it lends itself well to networking, where messages are exchanged to communicate.
On a conceptual level, a message indicates that either **something happened** or something **is requested to happen**.
To build upon this differentiation, we support two types of messages:
- An **event**, which designates that something happened within or outside the system. Events are sent in a **fire-and-forget** fashion, meaning the sender is not interested in knowing the outcome or side effects that the event caused, nor is the sender interested in the result.
  An example of an event may be a `StatisticEvent` or a `HeartbeatMessage`.
  The worker does not need to know what the coordinator does with the statistics or heartbeats.
  With respect to program flow, an event is always **asynchronous** and **non-blocking** from the perspective of the sender (although this is not 100% accurate in the presence of backpressure).
- A **request** is used in a request-response pattern.
  This indicates that the sender expects a result back.
  Examples of this may be `StartQuery` or `DropSource`.
  The user at least wants to know whether the desired effect of the message occurred (was the query actually started?), so it requires a result back from the coordinator.
  From the sender's perspective, a request can be either **synchronous**, **asynchronous**, **blocking**, or **non-blocking**.  
  What it will be in practice depends on the sender's preferences and its execution context.
  If the sender is located on the same node as the coordinator and runs in an async runtime, it has different requirements than a sender running on a different machine, using RPC to block on the response.

I expect events to be used primarily for internal communication in the direction worker --> coordinator, while the frontend --> coordinator communication might primarily use requests.
With that in mind, the decision also depends on whether the worker-coordinator-communication is pull- or push-based.
Hence, the coordinator should implement both.

From the top of my head, I can think of the following messages currently:
- Events:
    - `HealthUpdateEvent`/`HeartbeatEvent`, periodic small ping `StatisticEvent`, e.g., how many operators are currently running, current throughput
    - `RegisterWorkerEvent` the worker with the coordinator in the beginning, which may include information like the worker's capabilities in terms of CPU cores, cache sizes, etc.
- Requests:
    - `CreateLogicalSource`, `DropLogicalSource`, `GetLogicalSource`
    - `CreatePhysicalSource`, `DropPhysicalSource`, `GetPhysicalSource`
    - `CreateSink`, `DropSink`, `GetSink`
    - `CreateQuery`, `DropQuery`, `GetQuery`, `StartQuery`, `StopQuery`

We can further differentiate into internal and external messages, where internal ones originate from within the system, while external messages originate from the user or an external system.
The differentiation could be encoded into the implementation by using different ports/queues to receive them, handling them in different runtimes or with other priorities, i.e., internal first.

The API of the messaging interface should be as flexible as possible with respect to the **execution context** from which it is invoked.
The following requirements are of importance:
- Should be usable from normal threads and asynchronous runtimes.
  For example, the gRPC client that collects remote messages might run in an
  async runtime.
  In this case, the thread calling the send/ask API should never be blocked for an extended period of time.
  On the other hand, the REPL might be sending events from threads.
- Should hide whether the coordinator actually runs within the process where the send happens.
  If the coordinator is hosted on a remote machine, transparently forward the message via a network interface.
- Should be able to send all types of messages that adhere to a common interface, leaving room for future additions.

All event-typed messages should be sendable using the following variants:
1. `async fn send(event) -> Future<Result<(), SendError>>`: send event to coordinator from an async context. When the send would normally block, it returns a future that is resumed once ready to send.
2. `fn send_blocking(event) -> Result<(), SendError>`: send event to coordinator within the same processing from a normal threaded context.
3. Other extensions are possible, like `send_many` , `send_timeout` , or `try_send`

The send call does not wait for a response, allowing the sending thread to continue.
When the message bus reaches its batch size (explained in the next section), but the consumer is not finished processing, senders are blocked, either by awaiting the future or sleeping (addresses G5).
The backpressure propagates to remote sends via the network protocol.

All request-typed messages should be sendable using the following API:
1. `async fn ask(request) -> Future<Result<Response, AskError>>`: similarly, asking from an async context, the `Response` can be awaited via the returned future.
2. `fn ask(request) -> Result<Response, AskError>`: ask from threaded context, blocks (bringing the thread to sleep) until the response is ready.
   We might implement requests using RPC for inter-node communication.

## Architecture
<img width="1869" height="986" alt="coordinator_architecture(1)" src="https://github.com/user-attachments/assets/0fe92227-4eb2-4655-9aa3-817a73e5c16a" />

The architecture I envision divides the coordinator into three layers:
1. Messaging Layer
2. Service Layer
3. Data Layer

The **messaging layer** is responsible for gathering, dispatching, and storing messages that the coordinator receives and sends.
As described in the previous chapter, it supports transparent invocation from different execution contexts and from local as well as remote clients.
This flexibility allows us to receive data from multiple frontends and workers that may either reside within the current process or on a remote machine.
The transparency achieved through the messaging model will be beneficial when we want to transition to a more decentralized approach.
In this case, multiple coordinators that are responsible for a unique partition of the deployment can communicate with each other.
Similarly, in a setup with more autonomous workers, we can embed the coordinator library into the `SingleNodeWorker` and have it communicate with the local worker as well as remote embedded coordinators that are located upstream/downstream in the topology.
In the depicted figure, the REST server and Worker-1 are located on remote hosts while Worker-2 and the REPL/SLT tool run within the local process.
Furthermore, Worker-2 may want to respond to query requests from an asynchronous runtime where its RPC server runs, but the REPL wants to block on the response.
The messaging layer can be extended with preprocessing functionality (as shown in the diagram) for demultiplexing into topics (such as sources, sinks, queries, infrastructure, statistics, etc.) and compacting messages.
Finally, the changelog contains all changes that should be applied as part of the creation of the following snapshot.

The **service layer** processes the collected messages.
It controls the different entities of the deployment (sources, sinks, statistics, queries, workers) and applies all updates to the underlying data model.
In the service layer, we have data dependencies and therefore process messages in stages:
1. Infrastructure-related messages are processed first (they potentially influence queries, sources, sinks)
2. Source/Sink-related messages are processed second (they potentially influence queries)
3. Query-related messages are processed last (the snapshot includes up-to-date information on the other entities)

It might be possible and desirable that groups of related messages are processed together, and that stages emit new messages for downstream stages to include.
Therefore, the design of the changelog should facilitate this.
Within a single stage, there may be parallelization potential.

The **data layer** maintains the actual state of the coordinator.
We should decide on a single coherent data model that allows us to use a persistent database to store (parts of) a snapshot.
Current catalogs do not take into account that entities are related to each other, that deletes cascade, and that we may want to do more than point queries.
I am unsure what data models might be good fits to model the different entities we have, and if we could have a unified interface to query/update the metadata.
Finally, I am unsure how tightly/loosely coupled the processing stages and the data model should be.

## Program Flow
As I currently envision it, the coordinator conceptually runs an endless loop of **message gathering & message processing** passes, where processing is based on a snapshot of the system created by the previous pass.
- Message gathering collects messages, potentially from multiple threads and/or from over the network
- Message processing takes ownership of the gathered batch of messages and proceeds to create a new snapshot of the system

<img width="1342" height="540" alt="flow_coordinator drawio" src="https://github.com/user-attachments/assets/95d3250a-4c65-4278-aad4-d55909130b82" />

Both happen concurrently, synchronized by a thread-safe message bus abstraction.
It controls when a pass ends and a new one begins through two parameters: `batch_size` and `timeout`.
These two are upper bounds, the first one that triggers ends the current pass.
Note that there may be infinitely many senders, but only one receiver that claims the accumulated batch.
This does not mean that no parallelism is involved when processing the batch; instead, it means that a single receiver thread initially receives it.
The outlined idea combines two key mechanisms: **decoupling senders & receivers** with an n-to-1 mapping, and **timed batching**.
There are several advantages to this approach:
1. **Simplicity** from the perspective of the callers: senders are continuously pushing data into the coordinator without knowing about any barriers/snapshots.
   Senders do not know or care about the processing of their event/request.
   The receiver similarly runs in a loop, claiming a batch, processing it, and blocking until the next batch is ready.
   Addresses P1/G1.
5. **Unified approach for flow control**: Using the timed batch approach described, we can easily prevent overloading of the coordinator by blocking senders if the receiver has not finished processing.
   Blocking propagates the information that the coordinator is overloaded to the frontend or workers, even if they reside on another physical machine (over TCP/RPC). Addresses P2/G2.
6. **Trade-off throughput/latency**: By customizing the two key parameters, timeout and batch_size of the message bus, the deployment's throughput/latency characteristics can be configured.
   Under tighter latency constraints, lower the timeout. For maximum throughput, increase the batch size.
7. **Maximum flexibility for parallelism** in the query planner: since a single receiver receives the batch initially, it can spawn threads to parallelize work however it sees fit.
   For example, we could do logical optimization of the different queries in the batch in parallel. I think various strategies can be explored here, but to avoid premature optimization and allow for quick prototyping, it's best to start with a simple, single-threaded approach first.
8. **Cross-request optimization** potential: receiving a batch of requests enables us to perform tasks such as compacting requests that cancel each other out (create followed by drop of the same source/query).
   Another trick we could play is with query merging: if query requests within a single batch have a significant overlap, we can combine them and deploy only one or a few queries.

### Status Quo: no queueing, no batching

Currently, the threads that "produce" the events, i.e., serve the user requests, also invoke the query planner and send off the query to the worker.
This is not a viable approach for the reasons outlined in the problems section.
It brings the catalogs, binder, planner, and query manager into the frontends and leads to weird concurrency issues that are hard to solve.

### Alternative 1: multiple producers, multiple consumers (MPMC), no batching (instead use concurrency control mechanisms)

An MPMC message bus abstraction would allow for m consumer threads concurrently serving requests/events.
Modifications lead to the same issues as described above, because the approach towards parallelism is **unstructured** and not targeted to the use case (threads pick tasks blindly and execute them).
The degrees of freedom here are lower because **we do not control parallelism**, but only constrain it with locks/transactions.

### Alternative 2: SPSC/SPMC

Not viable, because we want to support multiple senders/producers.
Otherwise, we would only be able to support serving requests from a single frontend, from a single thread at once.

### Alternative 3: MPSC, no batching (continuous requests)

A possible approach that would allow for lower response times.
However, it limits parallelism to tasks that can be derived from a single request.
Also, we can approach this with the outlined preferred approach when lowering the timeout/batch size parameters anyway.

## Pull/Lazy vs. Push/Eager State Updates

When managing resources such as the cluster state or the queries currently running in the system, we need workers to report their local state.
These reports include information about their well-being, statistics, updated capabilities, outgoing network connections, and the status of local queries running on the worker.
We can obtain this information in one of two ways:
1. **Pull/Lazy**: We gather information lazily only when requested, for example, when a user queries the system or a deployment requires updated statistics.
4. **Push/Eager**: workers periodically send these messages proactively, and the coordinator continuously integrates them into the most recent snapshot.
   The pull-based approach can be implemented using RPC.
   It is particularly effective when updates are infrequent or concern relatively small parts of the global deployment state.
   In such cases, the overall number of messages exchanged within the system remains low.
   The downsides are:
- Every update requires an explicit request.
- Each request incurs a network round-trip, leading to higher latency for the user.

To the contrary, a push-based approach guarantees that when required, all pieces of information will be present in the last snapshot when needed.
This works well under a high load of incoming requests to the coordinator.
The required information will likely be accessible in this case, and no further network round-trip is necessary.
Here, the downsides are the opposite:
- Workers need to send updates frequently, burdening them with an additional load that might not actually be required.
    - However, when thinking towards the future, we want to re-optimize continuously anyway, requiring an updated state from more than the requested query.
- Similarly, the coordinator needs to integrate the state updates continuously into the following snapshot.

I am unsure what the better alternative would be, or whether a hybrid approach would be a reasonable starting point, depending on the message type.
One major drawback of the pull-based approach is that requests that are in-flight during the current pass depend on the outcome of the RPC calls.
This creates a choice: either the pass will be delayed until the last request in the batch is processed, or we defer incomplete requests to the next pass by resubmitting them into the message bus.
Example: a user wants to deploy a new query.
To plan the query, we require updated worker capabilities.
We send requests to all workers in the cluster, but if one message is lost, the planning phase is blocked until a response is eventually received.
After planning, the query must be deployed, which requires another round of communication with all nodes where parts of the query are assigned.
These round-trips accumulate quickly, resulting in substantial query deployment latency, especially in a large cluster with many machines.

**We should support both push- and pull-based communication between workers and the coordinator.
Especially when thinking about re-optimization passes, updated worker information is crucial (favoring a push-based approach).
Still, as a first baseline, we can extend the current gRPC machinery to request information pull-based and iterate from there.**

## Consistency
Instead of aiming at true/global consistency, we approach it with snapshots that are configurable in their granularity and behavior via the parameters mentioned.
Achieving true consistency in a distributed system is challenging and comes at a high cost.
With the proposed approach, the following things may happen:
- Requests are denied because conflicting requests (CREATE/DROP) arrived within the same pass.
- Queries are planned and deployed with out-of-date information within the configured timeouts.
- Every request receives a response at most as fast as when the current path ends, but probably later.

# 6. Programming Language
**I propose to write the coordinator in Rust.**
Because it is not dependent on the nautilus compiler in any way, this is generally possible.
C++ and Rust can interoperate via foreign function interface (FFI), which was first used in the distributed PoC.
In my opinion, there are lots of advantages to using Rust:
- Memory safety even in the presence of concurrency
- More ergonomic, improved error handling, and logging
- Very pleasant abstractions for sending data between threads
  (channels), exchanging messages via the network (serde crate, tokio networking library)
- Mature ecosystem for asynchronous networking applications with good documentation (compared to `boost::asio`)
- Rust's mighty enums make differentiating/dispatching different types of messages easy
- ...without sacrificing speed.

Architecturally, the coordinator sits between the frontends/workers, which act as producers of messages, and the current NebulI components of the `QueryManager`, including the catalogs and planning components that process the messages.
Therefore, we need to think about the language interaction C++ <--> Rust between the producers and consumers of the message bus.

### Interface Frontends/Worker -> Coordinator
The frontends and workers use the messaging interface to communicate with the coordinator.
This means that the FFI is restricted to a few functions in the `MessageBus`.
Messages themselves are required to be passed through the FFI boundary, which should be manageable since they will mostly contain primitive types or simple structures.
With respect to flow control and a C++ execution context invoking the functions of the `MessageBus`, events are trivial because we do not need to await a result.
Requests will need a conversion to something like `std::future` in cases where blocking is not desired.
The networking interface for receiving messages will be implemented in Rust, so no special care is required when a C++ worker/frontend wants to send a message to a remote coordinator.

### Interface Coordinator -> Query Planning
Because the coordinator manages the state of the deployment, the catalogs and the `QueryManager` will need to be ported to Rust, which is not a considerable amount of code.
The parser, binder, global optimizer, placer, and decomposer, which create and transform logical query plans, should not need to be ported, as this would require porting the whole machinery around the `LogicalOperator` and `LogicalPlan`, which is unnecessary in the current state of the system.
However, this means that we need to place an FFI boundary where Rust code of the coordinator calls into the C++ planner.
The resulting logical plan needs to be sent to a remote worker, so it needs to be passed back to the coordinator.
Regarding this, it would be easiest to pass a pointer instead of redefining the logical operator types on the Rust side to exchange the types by value.

### Alternative: C++ implementation
We need asynchronous networking to manage the communication within the cluster, leaving us most likely with `boost::asio` or `zmq`.
From my experience of working with `asio`, I much prefer the Rust ecosystem for the development of networking protocols and applications.
It is better documented and actively maintained while offering an integration with the core language `async/await` that is miles ahead of the integration between C++ coroutines and its networking libraries.
Moreover, this is an opportunity to move to a more modern replacement of C++ at a moment when it is possible.
If we let this opportunity pass and write 10K LoC in C++, there will likely be no further attempt in the coming 3-5 years.

# 7. Implementation Plan

Develop an MVP for the coordinator lib that resembles the current state of functionality and includes the following:
- MessageBus that stores and distributes messages
    - Sender handle for producers
    - Receiver to consume & handle batches
- Port catalogs to Rust
- Port `QueryManager` including gRPC client to Rust
- Single-threaded consumer pipeline that handles requests
    - No extensions like batch compaction, only messages to handle the current functionality (no infrastructure changes, etc.)
    - Dispatches them to handlers, i.e., the handler of a CREATE QUERY request would invoke the parser, planner, etc.
- Adapt binaries in the system to use the new lib, move out all unnecessary code to use the new messaging interface
    - Write FFI interop layer

# 8. Future Work
- Persistence of state (catalogs/topology) in an (embedded) database for enhanced fault tolerance
- Build testing infrastructure that goes beyond unit tests and our SLTs (possibly including fuzzing/property testing)
- Parallelization of message processing
- Support for shared query plans and query merging
