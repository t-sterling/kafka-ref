package com.processor.streams.old;

import com.processor.event.CdcEvent;
import com.processor.event.EntityRefreshed;
import com.processor.streams.state.GapRecordState;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;

/**
 * A per-recordId state machine implemented as a Kafka Streams Transformer.
 *
 * Why Transformer (not ValueTransformer)?
 * - Transformer has access to ProcessorContext.forward(...)
 * - That allows emitting MULTIPLE outputs for a single input (needed for "drain buffer on recovery")
 *
 * Input values:
 * - Normal CDC event: type "N" (or anything not "G"/"RECOVERY")
 * - GAP event: type "G"
 * - Recovery marker: type "RECOVERY" (synthetic event created from RecoveryEvent)
 *
 * Output values:
 * - Only normal CDC events that are safe to forward downstream.
 */
public class NormalizerTransformer implements Transformer<String, CdcEvent, KeyValue<String, CdcEvent>> {

    private static final Logger log = LoggerFactory.getLogger(NormalizerTransformer.class);

    private final String storeName;
    private final int maxBufferPerRecord;

    private ProcessorContext context;
    private KeyValueStore<String, GapRecordState> stateStore;

    public NormalizerTransformer(String storeName, int maxBufferPerRecord) {
        this.storeName = storeName;
        this.maxBufferPerRecord = maxBufferPerRecord;
    }

    /**
     * Convert a RecoveryEvent to a synthetic marker event so we can merge streams.
     */
    public static CdcEvent recoveryMarker(EntityRefreshed rec) {
        CdcEvent marker = new CdcEvent();
        marker.recordId = rec.recordId();
        marker.type = "RECOVERY";
        marker.timestamp = Instant.now().toString();
        marker.changedFields = Map.of("cutoffLastModifiedEpochMs", rec.cutoffLastModifiedEpochMs());
        return marker;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.stateStore = context.getStateStore(storeName);
        log.info("NormalizerTransformer initialized with state store={}", storeName);
    }

    @Override
    public KeyValue<String, CdcEvent> transform(String key, CdcEvent cdcEvent) {

        if (cdcEvent == null) {
            return null;
        }

        String recordId = key != null ? key : cdcEvent.recordId;
        if (recordId == null) {
            return null;
        }

        GapRecordState state = stateStore.get(recordId);
        if (state == null){
            state = new GapRecordState();
        }

        // ---- RECOVERY marker: exit GAP, set cutoff, drain buffer ----
        if ("RECOVERY".equalsIgnoreCase(cdcEvent.type)) {

            long cutoff = extractCutoff(cdcEvent);
            boolean wasInGap = state.inGap;
            int bufferedBefore = state.buffer.size();

            state.inGap = false;
            state.cutoffLastModifiedEpochMs = cutoff;

            log.warn("RECOVERY | recordId={} wasInGap={} cutoff={} bufferedBefore={} lastEmitted={}",
                    recordId, wasInGap, cutoff, bufferedBefore, state.lastEmittedLastModifiedEpochMs);

            int emitted = 0, stale = 0, dup = 0;

            while (!state.buffer.isEmpty()) {
                CdcEvent buffered = state.buffer.removeFirst();
                if (buffered == null) continue;
                if ("G".equalsIgnoreCase(buffered.type)) continue;

                long lm = extractLastModified(buffered);

                if (lm > 0 && lm < state.cutoffLastModifiedEpochMs) { stale++; continue; }
                if (lm > 0 && lm <= state.lastEmittedLastModifiedEpochMs) { dup++; continue; }

                // Forward buffered events downstream immediately
                context.forward(recordId, buffered);

                if (lm > 0) state.lastEmittedLastModifiedEpochMs = lm;
                emitted++;
            }

            log.warn("RECOVERY DONE | recordId={} emitted={} droppedStale={} droppedDup={} bufferedAfter={}",
                    recordId, emitted, stale, dup, state.buffer.size());

            stateStore.put(recordId, state);

            // We already forwarded buffered events via context.forward(...)
            // Returning null means "no extra output record from this call"
            return null;
        }

        // ---- GAP event: enter gap mode, do not emit downstream ----
        if ("G".equalsIgnoreCase(cdcEvent.type)) {
            boolean wasInGap = state.inGap;
            state.inGap = true;

            if (!wasInGap) {
                log.warn("GAP | entering GAP mode | recordId={} replayId={} cutoff={} lastEmitted={}",
                        recordId, cdcEvent.replayId, state.cutoffLastModifiedEpochMs, state.lastEmittedLastModifiedEpochMs);
            }

            stateStore.put(recordId, state);
            return null;
        }

        // ---- Normal event ----
        if (state.inGap) {
            state.buffer.addLast(cdcEvent);
            while (state.buffer.size() > maxBufferPerRecord) state.buffer.removeFirst();

            int sz = state.buffer.size();
            if (sz == 1 || sz % 25 == 0) {
                log.info("BUFFER | recordId={} bufferSize={} replayId={} lastModified={}",
                        recordId, sz, cdcEvent.replayId, extractLastModified(cdcEvent));
            }

            stateStore.put(recordId, state);
            return null;
        }

        // Not in gap => decide emit/drop
        long lm = extractLastModified(cdcEvent);

        if (lm > 0 && lm < state.cutoffLastModifiedEpochMs) {
            log.info("DROP STALE | recordId={} lastModified={} cutoff={} replayId={}",
                    recordId, lm, state.cutoffLastModifiedEpochMs, cdcEvent.replayId);
            stateStore.put(recordId, state);
            return null;
        }

        if (lm > 0 && lm <= state.lastEmittedLastModifiedEpochMs) {
            log.info("DROP DUP | recordId={} lastModified={} lastEmitted={} replayId={}",
                    recordId, lm, state.lastEmittedLastModifiedEpochMs, cdcEvent.replayId);
            stateStore.put(recordId, state);
            return null;
        }

        // Emit downstream
        if (lm > 0) state.lastEmittedLastModifiedEpochMs = lm;
        stateStore.put(recordId, state);

        log.debug("EMIT | recordId={} replayId={} lastModified={} changedKeys={}",
                recordId, cdcEvent.replayId, lm, (cdcEvent.changedFields == null ? "[]" : cdcEvent.changedFields.keySet()));

        // Returning KeyValue means this record will appear on the output KStream
        return KeyValue.pair(recordId, cdcEvent);
    }

    @Override
    public void close() {}

    private long extractLastModified(CdcEvent evt) {
        if (evt.changedFields == null) return 0L;
        Object v = evt.changedFields.get("lastModifiedEpochMs");
        if (v instanceof Number n) return n.longValue();
        if (v instanceof String s) {
            try { return Long.parseLong(s); } catch (Exception ignored) {}
        }
        return 0L;
    }

    private long extractCutoff(CdcEvent evt) {
        if (evt.changedFields == null) return 0L;
        Object v = evt.changedFields.get("cutoffLastModifiedEpochMs");
        if (v instanceof Number n) return n.longValue();
        if (v instanceof String s) {
            try { return Long.parseLong(s); } catch (Exception ignored) {}
        }
        return 0L;
    }
}
