package cdc.gap.handler.streams.state;

import cdc.gap.handler.domain.CdcEvent;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Per-record state for the normalizer state machine.
 * IMPORTANT:
 * This object is stored in a Kafka Streams state store.
 * That means it MUST be serializable (we will JSON-serialize it).
 * The key for this state is recordId (Employee record ID).
 */
public class GapRecordState {

    /**
     * True if we've seen a GAP for this record and are currently pausing output.
     */
    public boolean inGap = false;

    /**
     * After recovery, we drop any buffered events older than this cutoff.
     * (Used to prevent re-emitting stale CDC that occurred before refresh completed.)
     */
    public long cutoffLastModifiedEpochMs = 0L;

    /**
     * A basic de-dupe guard: only emit if event.lastModifiedEpochMs is > this.
     * This is not perfect in all real-world cases, but good for a simulator and
     * a solid pattern to study.
     */
    public long lastEmittedLastModifiedEpochMs = 0L;

    /**
     * While inGap==true, buffer normal events for this recordId.
     *
     * MUST be bounded, otherwise your state store grows without limit.
     * TODO: in practice what do we do with buffers that grow without limit ?
     *       if it hits the upper limit then we need a dead letter queue ?
     */
    public Deque<CdcEvent> buffer = new ArrayDeque<>();
}
