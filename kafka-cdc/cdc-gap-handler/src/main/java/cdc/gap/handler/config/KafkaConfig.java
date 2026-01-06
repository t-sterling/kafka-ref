package cdc.gap.handler.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import cdc.gap.handler.domain.CdcEvent;
import cdc.gap.handler.domain.Either;
import cdc.gap.handler.domain.FillEvent;
import cdc.gap.handler.domain.FillCommand;
import cdc.gap.handler.serde.JsonSerdes;
import cdc.gap.handler.streams.FillEventProcessor;
import cdc.gap.handler.streams.ForwarderProcessor;
import cdc.gap.handler.streams.GapEventProcessor;
import cdc.gap.handler.streams.state.GapRecordState;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.Properties;

@Configuration
public class KafkaConfig {

    private final GapHandlerProps gapHandlerProps;

    @Value("${spring.kafka.streams.application-id}")
    private String appId;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public KafkaConfig(GapHandlerProps appProps) {
        this.gapHandlerProps = appProps;
    }

    @Primary
    @Bean
    public Properties streamsProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        return props;
    }

    /**
     * This is the crucial difference vs the DSL/Spring auto setup:
     * - With @EnableKafkaStreams, Spring creates KafkaStreams for you.
     * - With a Topology + Processor API, YOU create KafkaStreams and
     *   let Spring manage its lifecycle (start/close).
     */
    @Bean(initMethod = "start", destroyMethod = "close")
    public KafkaStreams kafkaStreams(Topology topology, Properties streamsProperties) {
        return new KafkaStreams(topology, streamsProperties);
    }

    /**
     * Processor API: we build an explicit Topology graph:
     * source -> processor -> sink
     */
    @Bean
    public Topology topology(ObjectMapper mapper) {

        var recordIdSerde = Serdes.String();
        var cdcSerde = JsonSerdes.jsonSerde(mapper, CdcEvent.class);
        var reqSerde = JsonSerdes.jsonSerde(mapper, FillCommand.class);
        var refrehedSerdes = JsonSerdes.jsonSerde(mapper, FillEvent.class);

        var stateSerde = JsonSerdes.jsonSerde(mapper, GapRecordState.class);

        var topics = gapHandlerProps.topics();
        var topology = new Topology();

        // add cdc-in to the topology
        //
        topology.addSource(
            topics.cdcIn(),
            recordIdSerde.deserializer(),
            cdcSerde.deserializer(),
            topics.cdcIn()
        );

        // add the fill-event topic
        //
        topology.addSource(
            topics.cdcFillEvent(),
            recordIdSerde.deserializer(),
            refrehedSerdes.deserializer(),
            topics.cdcFillEvent()
        );

        // link a processor which recognizes gap-events and forks the stream
        //
        topology.addProcessor(
            "GapHandlerProcessor",
            () -> new GapEventProcessor(
                    gapHandlerProps.state().storeName(),
                    "CdcForwarder",
                    "FillCommandForwarder"
            ),
            topics.cdcIn()
        );

        // add processors that just forward the correct type to the correct topic
        //
        topology.addProcessor(
                "CdcForwarder",
                () -> new ForwarderProcessor<CdcEvent, CdcEvent, FillCommand>(Either::left, topics.cdcOut()),
                "GapHandlerProcessor"
        );
        topology.addProcessor(
                "FillCommandForwarder",
                () -> new ForwarderProcessor<FillCommand, CdcEvent, FillCommand>(Either::right, topics.cdcFillCommand()),
                "GapHandlerProcessor"
        );

        topology.addProcessor(
            "CdcFlusherProcessor",
            () -> new FillEventProcessor(gapHandlerProps.state().storeName(), topics.cdcOut()),
            topics.cdcFillEvent()
        );

        topology.addSink(
                topics.cdcOut(),
                topics.cdcOut(),
                recordIdSerde.serializer(),
                cdcSerde.serializer(),
                "CdcForwarder", "CdcFlusherProcessor"
        );

        topology.addSink(
                topics.cdcFillCommand(),
                topics.cdcFillCommand(),
                recordIdSerde.serializer(),
                reqSerde.serializer(),
                "FillCommandForwarder"
        );

        // Add a store for buffering CDC events while the FillEvent is being created
        //
        var storeBuilder = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(gapHandlerProps.state().storeName()),
            recordIdSerde,
            stateSerde
        );
        topology.addStateStore(storeBuilder, "GapHandlerProcessor", "CdcFlusherProcessor");

        return topology;
    }

}
