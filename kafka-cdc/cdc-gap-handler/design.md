# CDC Gap Handler Design

## What It Does

The **cdc-gap-handler** solves a common problem in CDC (Change Data Capture) systems: what happens when the source system has an outage and you receive a "gap" marker instead of the actual data?

### The Problem

When a source system (like a database) experiences an outage, it may send a special "gap event" (type `"G"`) instead of the actual change data. You can't just forward this gap marker downstream - you need the real data.

### The Solution

The gap handler acts as a buffer and coordinator:

1. **Normal flow (no gaps)**: CDC events flow straight through
   ```
   CDC-INPUT → GapEventProcessor → CDC-OUTPUT
   ```

2. **When a gap is detected** (event type = `"G"`):
   - Creates a `GapEventState` in a persistent state store (RocksDB)
   - Marks that record as "in gap" state
   - Sends a `FillCommand` to request the missing data asynchronously

3. **While waiting for the fill**:
   - Any subsequent CDC events for the same record ID get **buffered** in the state store
   - They won't be forwarded until the gap is resolved

4. **When the fill arrives** (via `CDC-FILL-EVENT` topic):
   - `FillEventProcessor` receives the filled data
   - Forwards the fill event to `CDC-OUTPUT`
   - Flushes all buffered events for that record to `CDC-OUTPUT`
   - Clears the state store entry

---

## Key Invariants

### 1. Co-partitioning by Record ID (CRITICAL)

`CDC-INPUT` and `CDC-FILL-EVENT` must be keyed by the same record ID. This ensures:
- The gap event and its corresponding fill event land on the **same partition**
- Therefore processed by the **same stream task/thread**
- The processor that created the buffer is the same one that flushes it

**Deployment Requirements:**
- Both topics MUST have the same number of partitions
- Both topics MUST use the same partitioning key (`recordId`)
- The producer partitioner MUST be consistent (default partitioner uses `murmur2(key) % numPartitions`)

**Failure Mode if Violated:**
If `CDC-FILL-EVENT` has different partition count or uses a different partitioner:
1. Gap event for `record-1` creates buffer in Task A (partition 0)
2. Fill event for `record-1` arrives at Task B (partition 1)
3. Task B looks up state, finds nothing, silently ignores the fill
4. Task A's buffer grows forever, never flushed
5. **Silent data loss** - no error, no alert, just stuck records

This cannot be caught by unit tests - must be validated at deployment.

### 2. Single-threaded State Store Access

Both `GapEventProcessor` and `FillEventProcessor` share `gap-state-store`. Because of co-partitioning, they're in the same stream task - no concurrent access, no locking needed.

**Why this works:** The topology has two source nodes (`CDC-INPUT`, `CDC-FILL-EVENT`) that would normally create separate sub-topologies. However, because they share a state store via `addStateStore()`, Kafka Streams merges them into a single sub-topology, ensuring single-threaded access.

### 3. Ordering Within a Record

Events for a given record ID are processed in offset order within that partition. This means:
- Buffered events are ordered by arrival time
- The fill event arrives "after" the gap (from the processor's perspective)

### 4. Idempotent Fill Processing

If a `FillEvent` arrives but there's no corresponding buffer in the state store:
- Either it was already processed (duplicate delivery)
- Or the gap timed out
- Either way, safe to drop (unless force-flag is set)

### 5. Timestamp-based Deduplication on Flush

When flushing buffered events, only events **newer than the fill's cutoff timestamp** are forwarded:

```java
if(cdcEvent.timestamp > record.value().cutoffLastModifiedEpochMs())
```

This prevents emitting stale CDC events that the fill already captured.

### 6. Bounded Buffers

The `GapEventState.buffer` must be bounded. Unbounded buffers risk:
- State store growing without limit
- Memory exhaustion

Punctuators should fail pending fills if too many events buffer or too much time passes.

### 7. At-Least-Once Semantics

The async fill service uses manual offset commits (ack after `FillEvent` published). This means:
- Fill commands may be processed multiple times on failure
- But that's safe because `FillEventProcessor` is idempotent

### 8. Gap Event Deduplication

If already in gap state for a record and another gap event arrives, only buffer normal events - duplicate gap markers are dropped. Only one `FillCommand` is sent per gap.

```java
if(!isGapEvent(cdcEvent)) {
    bufferCdc(cdcEvent);  // buffer normal events
} else {
    // drop duplicate gap events
}
```

---

## FillCommand Recovery

With `AT_LEAST_ONCE` processing, state store updates and FillCommand sends are not atomic.
If a crash occurs after state is committed but before FillCommand is written, the gap becomes orphaned.

### Recovery Mechanisms

**1. Startup Recovery**

When Kafka Streams transitions to `RUNNING` state, we scan the state store for any records
with `inGap=true` and re-send FillCommands via KafkaTemplate.

```java
streams.setStateListener((newState, oldState) -> {
    if (newState == KafkaStreams.State.RUNNING) {
        recoverOrphanedGaps(streams);
    }
});
```

**2. Punctuator Recovery**

A wall-clock punctuator runs periodically (configured via `stale-gap-check-interval-seconds`)
and checks for gaps older than `stale-gap-threshold-seconds`. Stale gaps get FillCommands re-sent.

```java
context.schedule(
    staleGapCheckInterval,
    PunctuationType.WALL_CLOCK_TIME,
    this::checkStaleGaps
);
```

### Why This Is Safe

Duplicate FillCommands result in duplicate FillEvents. The `FillEventProcessor` is idempotent:
- First FillEvent: flushes buffer, deletes state
- Duplicate FillEvent: state is null, nothing happens

The only cost of duplicates is wasted REST calls to the source system.

### Configuration

```yaml
gap-handler:
  state:
    stale-gap-check-interval-seconds: 60   # how often to check for stale gaps
    stale-gap-threshold-seconds: 120       # gaps older than this get FillCommand re-sent
```

---

## Failure Modes & Risks

### Silent Data Loss: Partition Mismatch
- **Cause:** `CDC-INPUT` and `CDC-FILL-EVENT` not co-partitioned
- **Symptom:** Buffers grow indefinitely, fills are silently ignored
- **Detection:** Monitor `gap-state-store` size, alert on entries older than max outage time
- **Prevention:** Validate partition counts match at startup or via admin tooling

### Lost FillCommand (MITIGATED)
- **Cause:** Crash between state store commit and FillCommand topic write
- **Symptom:** Gap stuck forever, buffer grows indefinitely
- **Mitigation:** Startup recovery + punctuator re-send stale FillCommands
- **Detection:** Monitor `cdc.fill-commands.resent` metric

### Unbounded Buffer Growth (TODO)
- **Cause:** Fill service down, slow, or failing
- **Symptom:** State store grows, memory pressure, potential OOM
- **Detection:** Monitor buffer sizes per record
- **Prevention:** Implement punctuator to timeout and fail gaps after max buffer size or time

### Ordering Violation
- **Cause:** Producer sends to wrong partition, or topic repartitioned
- **Symptom:** Events arrive out of order, potential data corruption
- **Prevention:** Use consistent keys, avoid topic repartitioning during operation
