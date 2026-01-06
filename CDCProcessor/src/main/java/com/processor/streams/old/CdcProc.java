/*
package com.processor.streams.old;

import com.processor.domain.CdcEvent;
import com.processor.domain.RecordState;
import org.apache.kafka.streams.processor.api.*;
import org.apache.kafka.streams.state.KeyValueStore;



public class CdcProc extends ContextualProcessor<String, CdcEvent, String, CdcEvent> {

    private final String storeName;
    private final int maxBuffer;
    private final String refreshRequestTopic;

    private KeyValueStore<String, RecordState> store;

    public CdcProc(String storeName, int maxBuffer, String refreshRequestTopic) {
        this.storeName = storeName;
        this.maxBuffer = maxBuffer;
        this.refreshRequestTopic = refreshRequestTopic;
    }

    @Override
    public void init(ProcessorContext<String, CdcEvent> context) {
        super.init(context);
        this.store = context.getStateStore(storeName);
    }

    @Override
    public void process(Record<String, CdcEvent> rec) {
        CdcEvent evt = rec.value();
        if (evt == null || evt.recordId == null) return;

        RecordState st = store.get(evt.recordId);
        if (st == null) st = new RecordState();

        if ("G".equalsIgnoreCase(evt.type)) {
            st.inGap = true;

            // request async refresh (key and value both recordId)
            context().forward(new Record<>(evt.recordId, evt.recordId, rec.timestamp()), refreshRequestTopic);

            // do NOT forward GAP downstream
            store.put(evt.recordId, st);
            return;
        }

        // Normal event: if in gap, buffer; else forward if eligible
        if (st.inGap) {
            buffer(st, evt);
            store.put(evt.recordId, st);
            return;
        }

        // Not in gap -> apply gating and forward
        if (eligible(st, evt)) {
            context().forward(new Record<>(evt.recordId, evt, rec.timestamp()));
            st.lastEmittedLastModifiedEpochMs = extractLastModified(evt);
        }

        store.put(evt.recordId, st);
    }

    private void buffer(RecordState st, CdcEvent evt) {
        st.buffer.addLast(evt);
        while (st.buffer.size() > maxBuffer) st.buffer.removeFirst();
    }

    private boolean eligible(RecordState st, CdcEvent evt) {
        long lm = extractLastModified(evt);
        if (lm <= 0) return true; // if missing, don’t block in sim
        if (lm < st.cutoffLastModifiedEpochMs) return false;
        return lm > st.lastEmittedLastModifiedEpochMs;
    }

    private long extractLastModified(CdcEvent evt) {
        if (evt.changedFields == null) return 0L;
        Object v = evt.changedFields.get("lastModifiedEpochMs");
        if (v instanceof Number n) return n.longValue();
        if (v instanceof String s) {
            try { return Long.parseLong(s); } catch (Exception ignored) {}
        }
        return 0L;
    }
}
*/
