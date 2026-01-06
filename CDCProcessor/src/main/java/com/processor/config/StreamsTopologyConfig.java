package com.processor.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.processor.event.CdcEvent;
import com.processor.event.EntityRefreshed;
import com.processor.serde.JsonSerdes;
import com.processor.streams.state.GapRecordState;
import com.processor.streams.old.NormalizerTransformer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;
import org.springframework.context.annotation.Bean;


public class StreamsTopologyConfig {

    // Topics (you can move these to @ConfigurationProperties later)
    private static final String INPUT = "SF-CDC";
    private static final String RECOVERY = "SF-CDC-RECOVERY";
    private static final String OUTPUT = "SF-CDC-OUTPUT";
    private static final String REFRESH_REQ = "SF-CDC-REFRESH-REQUEST";

    private static final String STORE_NAME = "record-state-store";

    @Bean
    public KStream<String, CdcEvent> normalizerStream(StreamsBuilder builder, ObjectMapper mapper) {

        // --- Serdes ---
        var keySerde = Serdes.String();
        var cdcSerde = JsonSerdes.jsonSerde(mapper, CdcEvent.class);
        var recoverySerde = JsonSerdes.jsonSerde(mapper, EntityRefreshed.class);
        var stateSerde = JsonSerdes.jsonSerde(mapper, GapRecordState.class);

        // --- State Store ---
        // This store holds RecordState per recordId.
        // Because it's persistent, Streams will back it with a changelog topic.
        StoreBuilder<KeyValueStore<String, GapRecordState>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STORE_NAME),
                        keySerde,
                        stateSerde
                );

        builder.addStateStore(storeBuilder);

        // --- Create the two input streams ---
        KStream<String, CdcEvent> cdcStream =
                builder.stream(INPUT, Consumed.with(keySerde, cdcSerde));

        KStream<String, EntityRefreshed> recoveryStream =
                builder.stream(RECOVERY, Consumed.with(keySerde, recoverySerde));

        // --- Side-output: refresh requests ---
        // This avoids any generic type problem: the refresh-request topic carries the GAP event itself.
        cdcStream
                .filter((k, v) -> v != null && "G".equalsIgnoreCase(v.type))
                .to(REFRESH_REQ, Produced.with(keySerde, cdcSerde));

        // --- Main normalization output ---
        //
        // We want ONE state machine per recordId that reacts to:
        //  - CDC events (N/G)
        //  - Recovery events (cutoff) -> drain buffer
        //
        // We unify the event types by mapping RecoveryEvent into a synthetic CdcEvent marker.
        KStream<String, CdcEvent> recoveryAsMarker =
                recoveryStream.mapValues(rec -> NormalizerTransformer.recoveryMarker(rec));

        KStream<String, CdcEvent> unified = cdcStream.merge(recoveryAsMarker);

        // Now run the state machine transformer.
        //
        // This transformer can emit multiple records using context.forward(...)
        // which is how we "drain all buffered events" immediately on recovery.
        KStream<String, CdcEvent> normalized =
                unified.transform(
                        () -> new NormalizerTransformer(STORE_NAME, 500),
                        STORE_NAME
                );

        // Write normalized events to OUTPUT
        normalized.to(OUTPUT, Produced.with(keySerde, cdcSerde));

        return normalized;
    }
}
