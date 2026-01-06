package com.processor.streams;

import com.processor.event.CdcEvent;
import com.processor.event.EntityRefreshed;
import com.processor.streams.state.GapRecordState;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

public class AsyncCompensatorProcessor extends ContextualProcessor<String, EntityRefreshed, Void, Void> {

    private KeyValueStore<String, GapRecordState> stateStore;
    private ProcessorContext<Void, Void> context;

    private final String stateStoreName;

    public AsyncCompensatorProcessor(String stateStoreName) {
        this.stateStoreName = stateStoreName;
    }

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        super.init(context);
        this.context = context;
        this.stateStore = context.getStateStore(this.stateStoreName);
    }

    @Override
    public void process(Record<String, EntityRefreshed> record) {

        var recordId = record.key();

        if(recordId != null){
            flush(recordId);
        }

    }

    private void flush(String recordId){

        var state = this.stateStore.get(recordId);
        if(state != null && state.buffer != null){
            while(!state.buffer.isEmpty()) {
                var cdcEvent = state.buffer.pop();
                publish(cdcEvent);
            }
            state.inGap = false;
        }

    }

    private void publish(CdcEvent event){
        System.err.println(event);
    }

}
