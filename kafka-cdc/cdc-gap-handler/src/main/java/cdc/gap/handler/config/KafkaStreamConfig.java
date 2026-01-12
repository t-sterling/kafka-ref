package cdc.gap.handler.config;

import cdc.gap.handler.streams.GapHandlerTopology;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.Properties;

@Configuration
public class KafkaStreamConfig {

    private final GapHandlerProps gapHandlerProps;

    @Value("${spring.kafka.streams.application-id}")
    private String appId;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public KafkaStreamConfig(GapHandlerProps appProps) {
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
    public Topology topology(ObjectMapper mapper, GapHandlerMetrics gapHandlerMetrics) {
        return new GapHandlerTopology().buildTopology(mapper, gapHandlerProps, gapHandlerMetrics);
    }

    @Bean
    GapHandlerMetrics gapHandlerMetrics(MeterRegistry meterRegistry){
        return new GapHandlerMetrics(meterRegistry);
    }

}
