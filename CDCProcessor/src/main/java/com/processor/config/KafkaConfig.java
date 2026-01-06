package com.processor.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.processor.event.CdcEvent;
import com.processor.serde.JsonSerdes;
import com.processor.streams.CdcProcessor;
import com.processor.streams.state.GapRecordState;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@EnableKafka
@Configuration
public class KafkaConfig {

    private static final String INPUT = "SF-CDC-INPUT";
    private static final String OUTPUT = "SF-CDC-OUTPUT";

    private static final String STORE_NAME = "cdc-state";



    /*
    @Bean
    public KStream<String, CdcEvent> buildTopology(ObjectMapper mapper){

        var keySerde = Serdes.String();
        var valSerde = JsonSerdes.jsonSerde(mapper, CdcEvent.class);

        var stateSerde = JsonSerdes.jsonSerde(mapper, GapRecordState.class);

        StoreBuilder<KeyValueStore<String, GapRecordState>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STORE_NAME),
                        keySerde,
                        stateSerde
                );

        builder.addStateStore(storeBuilder);

        var stream = builder.stream(INPUT, Consumed.with(keySerde, valSerde));
        stream.process(
                    () -> new CdcProcessor(STORE_NAME),
                    Named.as("cdc-processor"),
                    STORE_NAME
                )
              .to(OUTPUT, Produced.with(keySerde, valSerde));

        return stream;

    }
     */
}
