package cdc.gap.handler.streams;

import cdc.gap.handler.config.GapHandlerMetrics;
import cdc.gap.handler.domain.CdcEvent;

import cdc.gap.handler.domain.Either;
import cdc.gap.handler.domain.FillCommand;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cdc.gap.handler.config.GapHandlerMetrics.*;

/**
 * This processes incoming cdc-events
 * if they are gap events:
 *  - buffer in the state-store
 *  - sent a FillCommand to the forwarder
 * else:
 *  - forward to the cdc-event forwarder
 */
public class GapEventProcessor extends ContextualProcessor<String, CdcEvent, String, Either<CdcEvent, FillCommand>> {

    private static final Logger LOG = LoggerFactory.getLogger(GapEventProcessor.class);

    private final String stateStoreName;
    private final String cdcForwarder;
    private final String fillCommandForwarder;
    private final GapHandlerMetrics metrics;

    private KeyValueStore<String, GapEventState> stateStore;
    private ProcessorContext<String, Either<CdcEvent, FillCommand>> context;

    public GapEventProcessor(String stateStoreName,
                             String cdcForwarder,
                             String fillCommandForwarder,
                             GapHandlerMetrics gapHandlerMetrics) {
        this.stateStoreName = stateStoreName;
        this.cdcForwarder = cdcForwarder;
        this.fillCommandForwarder = fillCommandForwarder;
        this.metrics = gapHandlerMetrics;
    }

    @Override
    public void init(ProcessorContext<String, Either<CdcEvent, FillCommand>> context) {
        super.init(context);
        this.context = context;
        this.stateStore = context.getStateStore(this.stateStoreName);
    }

    @Override
    public void process(Record<String, CdcEvent> record) {

        this.metrics.count(GapHandlerMetrics.CDC_EVENT_RECEIVED_COUNT);

        if(isValidRecord(record)){

            var cdcEvent = record.value();

            if(isInGap(cdcEvent)){

                // if a gap event is already in flight don't trigger another fill command
                //
                if(!isGapEvent(cdcEvent)) {
                    LOG.info("buffer: {}/{}", cdcEvent.recordId, cdcEvent.eventId);
                    bufferCdc(cdcEvent);
                } else {
                    metrics.count(CDC_GAP_EVENT_DETECTED_COUNT);
                    metrics.count(CDC_GAP_EVENT_DROPPED);
                }

            } else if(isGapEvent(cdcEvent)){

                LOG.info("publish fill-command: {}/{}", cdcEvent.recordId, cdcEvent.eventId);
                metrics.count(CDC_GAP_EVENT_DETECTED_COUNT);
                sendFillCommand(record);

            } else {

                forwardCdcEvent(record);

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


    /**
     * Requests a refresh for a given CDC event record. If the associated `recordId`
     * does not exist in the state store, it initializes a new `GapRecordState`, marks
     * it as being in a gap state, and buffers the event. The method then constructs and
     * forwards a `FillCommand` encapsulated in an `Either` object to handle the gap.
     *
     * @param record the CDC event record to process, an instance of {@code Record<String, CdcEvent>}
     */
    private void sendFillCommand(Record<String, CdcEvent> record){
        var evt = record.value();
        if(this.stateStore.get(evt.recordId) == null) {
            var state = new GapEventState();
            state.inGap = true;
            this.stateStore.put(evt.recordId, state);
        }
        var forwardRecord = record.withValue(new Either<CdcEvent, FillCommand>(
            null,
            new FillCommand(evt.recordId, evt.entity, evt.eventId)
        ));
        this.context.forward(forwardRecord, this.fillCommandForwarder);
        this.metrics.count(GapHandlerMetrics.CDC_FILL_COMMAND_PUBLISHED_COUNT);

    }

    private void failFillEvent(String recordId){
        // TODO: pipe to a replay topic or db -  we need a button somewhere for replay
        this.stateStore.delete(recordId);
        LOG.warn("Failed to fill event for recordId: {}", recordId);
        this.metrics.count(CDC_FILL_COMMAND_TIMEOUT);
    }

    /**
     * Buffers a CDC event into a per-record buffer for handling events while in a gap state.
     * The event is retrieved and added to the buffer associated with its `recordId`.
     *
     * @param event the CDC event to buffer, an instance of {@code CdcEvent}
     */
    private void bufferCdc(CdcEvent event) {
        var state = this.stateStore.get(event.recordId);
        state.buffer.add(event);
        this.stateStore.put(event.recordId, state);
        this.metrics.count(GapHandlerMetrics.CDC_EVENT_BUFFERED_COUNT);
    }

    /**
     * Forwards a CDC event encapsulated within the given record to the configured
     * CDC forwarder context in an enriched format.
     *
     * The method wraps the original {@code CdcEvent} value from the record within an
     * {@code Either} instance, where the first parameter is the CDC event itself and the
     * second parameter is {@code null}. This wrapped object is then forwarded to the
     * configured downstream processor using the associated {@code cdcForwarder}.
     *
     * @param record the CDC event record to forward, an instance of {@code Record<String, CdcEvent>}
     */
    private void forwardCdcEvent(Record<String, CdcEvent> record){
        var forwardRecord = record.withValue(new Either<CdcEvent, FillCommand>(record.value(), null));
        this.context.forward(forwardRecord, this.cdcForwarder);
        this.metrics.count(GapHandlerMetrics.CDC_EVENT_FORWARDED_COUNT);
    }

}
