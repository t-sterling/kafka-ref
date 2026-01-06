package com.processor.streams;

import com.processor.event.CdcEvent;

import com.processor.streams.state.GapRecordState;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This represents the normal flow
 */
public class CdcProcessor extends ContextualProcessor<String, CdcEvent, String, CdcEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(CdcProcessor.class);

    private final String stateStoreName;

    private KeyValueStore<String, GapRecordState> stateStore;
    private ProcessorContext<String, CdcEvent> context;

    public CdcProcessor(String stateStoreName) {
        this.stateStoreName = stateStoreName;
    }

    @Override
    public void init(ProcessorContext<String, CdcEvent> context) {
        super.init(context);
        this.context = context;
        this.stateStore = context.getStateStore(this.stateStoreName);
    }

    @Override
    public void process(Record<String, CdcEvent> record) {

        if(isValidRecord(record)){

            var cdcEvent = record.value();

            if(isInGap(cdcEvent)){

                LOG.info("In gap state: {}", cdcEvent);

                bufferCdc(cdcEvent);

            } else if(isGapEvent(cdcEvent)){

                LOG.info("Gap event detected: {}", cdcEvent);

                requestRefresh(cdcEvent);

            } else {

                this.context.forward(record);

            }

        }

    }

    /**
     * Validates whether the given record is a valid.
     *
     * @param record the record to validate, an instance of {@code Record<String, CdcEvent>}
     * @return true if the record meets the validity conditions; false otherwise
     */
    private boolean isValidRecord(Record<String, CdcEvent> record) {
        return record != null
                && record.key() != null
                && record.value() != null
                && record.value().recordId != null;
    }

    /**
     * Determines if the given CDC event is currently in a gap state. A record is considered
     * in a gap if the `inGap` flag within its associated {@code RecordState} in the state store is true.
     *
     * @param evt the CDC event to check, an instance of {@code CdcEvent}
     * @return true if the record associated with the given CDC event is in a gap; false otherwise
     */
    private boolean isInGap(CdcEvent evt) {
        var gapState = this.stateStore.get(evt.recordId);
        if(gapState == null) {
            return false;
        }
        return gapState.inGap;
    }

    /**
     * Determines if the given CDC event is categorized as a gap event.
     * A gap event is identified when the `type` field of the event has
     * the value "G" (case insensitive).
     *
     * @param evt the CDC event to evaluate, an instance of {@code CdcEvent}
     * @return true if the event is a gap event (type "G"); false otherwise
     */
    private boolean isGapEvent(CdcEvent evt) {
        return evt.type.equalsIgnoreCase("G");
    }

    private void requestRefresh(CdcEvent evt){
        if(this.stateStore.get(evt.recordId) == null) {
            var state = new GapRecordState();
            state.inGap = true;
            state.buffer.add(evt);
            this.stateStore.put(evt.recordId, state);
        }
        // TODO: publish refresh request
    }


    /**
     * Buffers a CDC event into a per-record buffer for handling events while in a gap state.
     * The event is retrieved and added to the buffer associated with its `recordId`.
     *
     * @param evt the CDC event to buffer, an instance of {@code CdcEvent}
     */
    private void bufferCdc(CdcEvent evt) {
        var state = this.stateStore.get(evt.recordId);
        state.buffer.add(evt);
        this.stateStore.put(evt.recordId, state);
    }


}
