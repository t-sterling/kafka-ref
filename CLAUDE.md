# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Kafka Change Data Capture (CDC) reference implementation demonstrating gap detection and buffering patterns for event-driven architectures. It uses Kafka Streams Processor API (not DSL) with Spring Boot 3.3.3 and Java 21.

## Build Commands

```bash
# Build all modules
mvn clean install

# Build specific module
mvn clean install -pl kafka-cdc/cdc-gap-handler

# Run tests
mvn test

# Run single test
mvn test -Dtest=KafkaStreamsTest -pl kafka-cdc/cdc-gap-handler

# Run specific module
mvn spring-boot:run -pl kafka-cdc/cdc-gap-handler
```

## Architecture

### Module Structure

- **kafka-cdc/cdc-common** - Shared domain models (Account, Contact, Deal, Opportunity, etc.)
- **kafka-cdc/cdc-simulator** (port 8085) - Generates synthetic CDC events with configurable gap injection
- **kafka-cdc/cdc-gap-handler** (port 8083) - Core gap detection using Kafka Streams Processor API
- **kafka-cdc/cdc-compensator** - Async worker consuming FillCommand events
- **kafka-cdc/cdc-formula-materializer** (port 8086) - Redis-based formula field materialization

### Gap Handler Topology

```
CDC-INPUT ──> GapEventProcessor ──> CDC-OUTPUT (normal events)
                     │
                     └──> FillCommand (gap detected)
                             │
                             ▼ (async service)
                        CDC-FILL-EVENT
                             │
                             ▼
                    FillEventProcessor ──> CDC-OUTPUT (filled events)
                                       └──> ERROR (if fill failed)
```

Key files:
- `cdc-gap-handler/src/main/java/cdc/gap/handler/streams/GapHandlerTopology.java` - Topology builder
- `cdc-gap-handler/src/main/java/cdc/gap/handler/streams/GapEventProcessor.java` - Gap detection logic
- `cdc-gap-handler/src/main/java/cdc/gap/handler/streams/FillEventProcessor.java` - Fill response handling

### Key Patterns

1. **Kafka Streams Processor API** - Uses low-level Processor API for explicit topology control with custom state store access
2. **State Store Buffering** - In-flight events buffered in RocksDB-backed `gap-state-store` during gap fills
3. **Manual Offset Commit** - FillCommand consumer uses manual commits for at-least-once semantics
4. **Co-partitioning** - CDC-INPUT and CDC-FILL-EVENT keyed by record ID for single-threaded processing
5. **Backpressure Control** - `KafkaBackPressureController` pauses consumer without rebalance on transient errors

## Infrastructure

Start services with Docker Compose:

```bash
# Kafka cluster (3 brokers + Zookeeper + Kafka-UI)
docker-compose -f infra/kafka/docker-compose.yml up

# Redis + RedisInsight
docker-compose -f infra/redis/docker-compose.yml up

# Prometheus + Grafana
docker-compose -f infra/observability/docker-compose.yml up
```

Ports:
- Kafka brokers: 29092, 29093, 29094
- Kafka-UI: 8080
- Redis: 6379, RedisInsight: 5540
- Prometheus: 9090, Grafana: 3000

## Configuration

Configuration uses `@ConfigurationProperties` with typed records. Main config class: `GapHandlerProps`.

Key application.yml settings in `cdc-gap-handler`:
- `gap-handler.topics.*` - Topic names (CDC-INPUT, CDC-OUTPUT, CDC-FILL-COMMAND, CDC-FILL-EVENT)
- `gap-handler.state.store-name` - State store name
- `gap-handler.retries.*` - Exponential backoff settings

## Testing

Uses `TopologyTestDriver` for Kafka Streams topology unit tests:

```bash
mvn test -pl kafka-cdc/cdc-gap-handler -Dtest=KafkaStreamsTest
```

## Additional Documentation

- `kafka-cdc/readme.md` - CDC project overview
- `kafka-cdc/cdc-gap-handler/readme.md` - Detailed topology with Mermaid diagrams
- `kafka-cdc/cdc-common/src/main/java/cdc/domain/readme.md` - Domain entity ER diagram
