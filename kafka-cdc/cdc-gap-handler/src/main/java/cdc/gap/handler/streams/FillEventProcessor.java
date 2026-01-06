package cdc.gap.handler.streams;

import cdc.gap.handler.domain.CdcEvent;
import cdc.gap.handler.domain.FillEvent;
import cdc.gap.handler.streams.state.GapRecordState;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FillEventProcessor extends ContextualProcessor<String, FillEvent, String, CdcEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(FillEventProcessor.class);

    private KeyValueStore<String, GapRecordState> stateStore;
    private final String outputTopic;

    private ProcessorContext<String, CdcEvent> context;

    private final String stateStoreName;

    public FillEventProcessor(String stateStoreName, String outputTopic) {
        this.stateStoreName = stateStoreName;
        this.outputTopic = outputTopic;
    }

    @Override
    public void init(ProcessorContext<String, CdcEvent> context) {
        super.init(context);
        this.context = context;
        this.stateStore = context.getStateStore(this.stateStoreName);
    }

    @Override
    public void process(Record<String, FillEvent> record) {
        var recordId = record.key();
        if(recordId != null){
            flush(record);
        }
    }

    private void flush(Record<String, FillEvent> record){
        var recordId = record.key();
        var state = this.stateStore.get(recordId);
        if(state != null && state.buffer != null){
            LOG.info("Flushing {} events for {}", state.buffer.size(), recordId);

            // send a fake cdc 'fill event' which just includes all fields
            //
            var fillEvent = createFillEvent(record.value());
            this.context.forward(record.withValue(fillEvent), this.outputTopic);

            // now replay anything in the cdc buffer more recent
            //
            while(!state.buffer.isEmpty()) {

                var cdcEvent = state.buffer.pop();
                // only flush records with a greater timestamp
                if(cdcEvent.timestamp > record.value().cutoffLastModifiedEpochMs()) {
                    this.context.forward(record.withValue(cdcEvent), this.outputTopic);
                }
            }
            // remove the state - we don't need anymore
            this.stateStore.delete(recordId);
            LOG.info("Flushed {} events for {}", state.buffer.size(), recordId);
        }

    }

    CdcEvent createFillEvent(FillEvent fillEvent){

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
