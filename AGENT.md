# Kafka Simulator MVP Implementation Opportunities

## Project Positioning

Build a browser-only Kafka learning simulator comparable in spirit to `tryrabbitmq.com`.
This is not a real Kafka broker, client, protocol implementation, database-backed app, or admin console.
The MVP should help learners understand Kafka's core runtime model through direct manipulation:

- Producers write records to topics.
- Topics are split into partitions.
- Records are appended with offsets.
- Keys influence partition selection and ordering.
- Consumers read by offset.
- Consumer groups share partition ownership and maintain committed offsets.
- Lag changes as records are produced, polled, and committed.

All state should live in front-end memory, `localStorage`, or imported/exported JSON.

## Benchmark Notes

The RabbitMQ simulator pattern to match is the interaction model, not the domain model:

- Left toolbox of draggable messaging elements.
- Central canvas for topology construction.
- Connections between elements.
- Right-side properties/actions for the selected element.
- Bottom message/activity log.
- Simple mode plus advanced controls.
- JSON import/export or player data for reproducible scenarios.

Kafka concepts should remain Kafka-native. Do not force RabbitMQ concepts such as exchange, queue, or binding into the design.

## P0: Required MVP Opportunities

### 1. Visual Workbench

Create a single-page simulator with:

- Toolbox elements: Producer, Topic, Consumer Group, Consumer, Broker/Cluster.
- Canvas placement, selection, rename, delete, and reposition support.
- Valid connections:
  - Producer -> Topic
  - Topic -> Consumer Group
  - Consumer Group -> Consumer
  - Topic -> Consumer, only if the app supports an implicit single-member group
- Right properties panel that changes by selected element.
- Bottom event log that records every meaningful simulation action.

Recommended stack: React, TypeScript, Vite, React Flow, Zustand.

### 2. Pure Front-End Simulation Engine

Implement a deterministic state model independent from the UI:

- `Topic`: name, partitions, retention mode placeholder, replication placeholder.
- `Partition`: id, records, next offset.
- `Record`: offset, key, value, timestamp, headers, producer id.
- `Producer`: id, name, partition strategy.
- `ConsumerGroup`: id, group id, subscribed topics, members, assignment, committed offsets.
- `Consumer`: id, name, group id, current positions.
- `SimulationEvent`: timestamp, type, message, related entity ids.

The simulation engine should expose pure operations for produce, poll, commit, seek, reset offset, add/remove members, and rebalance.

### 3. Produce Records

Producer panel must support:

- Topic selection from connected topics.
- Key input.
- Value input.
- Optional headers input.
- Partition strategy:
  - Round robin.
  - Key hash.
  - Explicit partition.
- Single send action.
- Optional timed sending can be added if it stays simple.

The UI should show which partition was selected and why.

### 4. Topic and Partition Visualization

Topic nodes must show:

- Partition count.
- Per-partition append-only log.
- Each record's offset, key, and short value preview.
- Highlight of the latest produced record.
- Clear indication that consumption does not delete records.

Topic properties should allow changing partition count before meaningful records exist. After records exist, changing partition count can be disabled for MVP.

### 5. Consumer Group Semantics

Consumer group panel must show:

- Members in the group.
- Topic subscriptions.
- Partition assignment per member.
- Current position per topic-partition.
- Committed offset per topic-partition.
- Lag per topic-partition and total lag.

Required actions:

- Add consumer.
- Remove consumer.
- Poll one record.
- Poll batch.
- Commit current offsets.
- Seek to offset.
- Reset to earliest.
- Reset to latest.

Changing group membership must trigger a visible rebalance event and update assignments.

### 6. Message and Activity Log

The log should explain simulator behavior in learner-friendly terms:

- Produced record to `topic[partition]@offset`.
- Partition selected by key hash, round robin, or explicit setting.
- Consumer polled record from `topic[partition]@offset`.
- Offsets committed.
- Consumer group rebalanced.
- Lag increased or decreased.
- Seek/reset performed.

Avoid turning the log into debug output. It should teach the model.

### 7. Scenario Persistence

Support:

- Save current scenario to `localStorage`.
- Load saved scenario.
- Export scenario as JSON.
- Import scenario from JSON.
- Reset to blank workspace.

Exported JSON should include topology, records, offsets, positions, selected mode, and log history.

### 8. Built-In Learning Scenarios

Ship at least these starter scenarios:

- Basic produce and consume.
- Key-based ordering across partitions.
- Multiple consumers in one group sharing partitions.
- Two consumer groups reading the same topic independently.
- Consumer lag and manual commit.
- Seek and replay from an earlier offset.

Each scenario should be loaded directly into the simulator, not displayed as a static tutorial page.

## P1: High-Value Follow-Ups

- Broker node view with partition leaders and follower replicas.
- Replication factor and ISR visualization.
- Producer `acks=0`, `acks=1`, and `acks=all` behavior at a simplified level.
- Broker failure toggle and leader failover animation.
- Retention by max records or simulated time.
- Log compaction mode for keyed records.
- Dead letter style learning scenario as a Kafka pattern, clearly labeled as application-level behavior.
- Shareable scenario URLs using compressed state in the hash.
- Guided challenge mode with small tasks and validation checks.

## P2: Out of Scope Until MVP Works

- Real Kafka protocol.
- Backend services.
- Database persistence.
- Authentication.
- Multi-user collaboration.
- Kafka Connect.
- Kafka Streams DSL.
- Schema Registry.
- Transactions and exactly-once semantics.
- ACL/security modeling.
- Full KRaft/controller internals.
- Production admin-console features.

## UX Principles

- The first screen should be the simulator itself, not a landing page.
- Favor dense, inspectable operational UI over marketing-style sections.
- Use icons for simulator tools and compact controls for repeated actions.
- Do not explain the app with large static text blocks inside the main workspace.
- Put explanations where they are actionable: selected properties panel, event log, tooltips, and scenario hints.
- Make every state transition visible: append, poll, commit, rebalance, lag change, seek, reset.

## Acceptance Criteria

The MVP is acceptable when a learner can complete these flows with no backend:

1. Create a producer, topic with three partitions, consumer group, and two consumers.
2. Produce records with and without keys and see partition selection.
3. Observe records appended to partitions with offsets.
4. Poll records from the consumer group and see current offsets move.
5. Commit offsets and see lag update.
6. Add or remove a consumer and see a rebalance.
7. Seek backward and replay records without deleting them.
8. Export the scenario JSON, reset, import it, and continue from the same state.
