package cdc.gap.handler.config;

import cdc.gap.handler.domain.FillCommand;
import cdc.gap.handler.streams.GapEventState;
import cdc.gap.handler.streams.GapHandlerTopology;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Properties;
import java.util.UUID;

@Configuration
public class KafkaStreamConfig {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamConfig.class);

    private final GapHandlerProps gapHandlerProps;
    private final KafkaTemplate<String, FillCommand> fillCommandKafkaTemplate;

    @Value("${spring.kafka.streams.application-id}")
    private String appId;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public KafkaStreamConfig(GapHandlerProps appProps,
                             KafkaTemplate<String, FillCommand> fillCommandKafkaTemplate) {
        this.gapHandlerProps = appProps;
        this.fillCommandKafkaTemplate = fillCommandKafkaTemplate;
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
        var streams = new KafkaStreams(topology, streamsProperties);

        // on startup, recover any orphaned gaps by re-sending FillCommands
        //
        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING) {
                recoverOrphanedGaps(streams);
            }
        });

        return streams;
    }

    /**
     * Scans the state store for any records stuck in gap state and re-sends FillCommands.
     * This handles the case where a FillCommand was lost due to a crash between
     * state store commit and topic write.
     */
    private void recoverOrphanedGaps(KafkaStreams streams) {
        try {
            var storeName = gapHandlerProps.state().storeName();
            var fillCommandTopic = gapHandlerProps.topics().cdcFillCommand();

            ReadOnlyKeyValueStore<String, GapEventState> store = streams.store(
                StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore())
            );

            int recoveredCount = 0;
            try (var iter = store.all()) {
                while (iter.hasNext()) {
                    var entry = iter.next();
                    var recordId = entry.key;
                    var state = entry.value;

                    if (state.inGap) {
                        LOG.info("recovering orphaned gap on startup: {}", recordId);
                        var fillCommand = new FillCommand(
                            recordId,
                            state.entityType,
                            "startup-" + UUID.randomUUID()
                        );
                        fillCommandKafkaTemplate.send(fillCommandTopic, recordId, fillCommand);
                        recoveredCount++;
                    }
                }
            }

            if (recoveredCount > 0) {
                LOG.info("recovered {} orphaned gaps on startup", recoveredCount);
            }

        } catch (Exception e) {
            LOG.error("failed to recover orphaned gaps on startup", e);
        }
    }

    /**
     * Processor API: we build an explicit Topology graph:
     * source -> processor -> sink
     */
    @Bean
    public Topology topology(ObjectMapper mapper, GapHandlerMetrics gapHandlerMetrics) {
        return new GapHandlerTopology().buildTopology(mapper, gapHandlerProps, gapHandlerMetrics);
    }

    @Bean
    GapHandlerMetrics gapHandlerMetrics(MeterRegistry meterRegistry){
        return new GapHandlerMetrics(meterRegistry);
    }

}
