package cdc.gap.handler.streams;

import cdc.gap.handler.config.GapHandlerMetrics;
import cdc.gap.handler.domain.CdcEvent;
import cdc.gap.handler.domain.FillEvent;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cdc.gap.handler.config.GapHandlerMetrics.CDC_EVENT_FILLED_COUNT;
import static cdc.gap.handler.config.GapHandlerMetrics.CDC_FILL_EVENT_RECEIVED_COUNT;

public class FillEventProcessor extends ContextualProcessor<String, FillEvent, String, CdcEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(FillEventProcessor.class);

    private final String cdcOutput;
    private final String stateStoreName;
    private final GapHandlerMetrics metrics;

    private KeyValueStore<String, GapEventState> stateStore;
    private ProcessorContext<String, CdcEvent> context;

    public FillEventProcessor(String stateStoreName,
                              String cdcOuput,
                              GapHandlerMetrics metrics) {
        this.stateStoreName = stateStoreName;
        this.cdcOutput = cdcOuput;
        this.metrics = metrics;
    }

    @Override
    public void init(ProcessorContext<String, CdcEvent> context) {
        super.init(context);
        this.context = context;
        this.stateStore = context.getStateStore(this.stateStoreName);
    }

    @Override
    public void process(Record<String, FillEvent> record) {
        this.metrics.count(CDC_FILL_EVENT_RECEIVED_COUNT);
        var recordId = record.key();
        if(recordId != null){
            flush(record);
        }
    }

    private void flush(Record<String, FillEvent> record){

        LOG.info("flush: {}/{}", record.value().recordId(), record.value().eventId());
        var recordId = record.key();
        var state = this.stateStore.get(recordId);
        if(state != null && state.buffer != null){

            // send a fake cdc 'fill event' which just includes all fields
            //
            var fillEvent = createFillEvent(record.value());
            this.context.forward(record.withValue(fillEvent), this.cdcOutput);
            this.metrics.count(CDC_EVENT_FILLED_COUNT);

            // now replay anything in the cdc buffer more recent
            //
            while(!state.buffer.isEmpty()) {

                var cdcEvent = state.buffer.pollFirst();
                // only flush records with a greater timestamp
                if(cdcEvent.timestamp > record.value().cutoffLastModifiedEpochMs()) {
                    this.context.forward(record.withValue(cdcEvent), this.cdcOutput);
                    this.metrics.count(GapHandlerMetrics.CDC_EVENT_FLUSHED_COUNT);
                } else {
                    this.metrics.count(GapHandlerMetrics.CDC_EVENT_DROPPED_COUNT);
                }
            }
            // remove the state - we don't need anymore
            this.stateStore.delete(recordId);
        }

    }

    private CdcEvent createFillEvent(FillEvent fillEvent){

        // Create a new CdcEvent instance
        CdcEvent cdcEvent = new CdcEvent();

        // Populate the fields in CdcEvent using fillEvent fields
        cdcEvent.recordId = fillEvent.recordId();
        cdcEvent.entity = fillEvent.recordId();
        cdcEvent.changedFields = fillEvent.allFields();
        cdcEvent.type = "F";

        return cdcEvent;
    }

}
