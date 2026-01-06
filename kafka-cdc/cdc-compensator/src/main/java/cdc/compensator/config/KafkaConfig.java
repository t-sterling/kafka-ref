package cdc.compensator.config;

import cdc.compensator.domain.FillEvent;
import cdc.compensator.domain.FillCommand;
import cdc.compensator.service.CompensatorService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.web.client.RestTemplate;


@EnableKafka
@Configuration
public class KafkaConfig {

    private final CompensatorProps compensatorProps;

    public KafkaConfig(CompensatorProps compensatorProps) {
        this.compensatorProps = compensatorProps;
    }

    /**
     * listen to RefreshEntityCommands
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, FillCommand> kafkaListenerContainerFactory(
            ConsumerFactory<String, FillCommand> consumerFactory) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, FillCommand>();
        factory.setConsumerFactory(consumerFactory);
        return factory;
    }

    /**
     * publish EntityRefreshedEvents
     */
    @Bean
    public KafkaTemplate<String, FillEvent> kafkaTemplate(ProducerFactory<String, FillEvent> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    CompensatorService compensatorService(KafkaTemplate<String, FillEvent> kafkaTemplate,
                                          RestTemplate restTemplate,
                                          ObjectMapper objectMapper) {
        return new CompensatorService(
            this.compensatorProps.topics().cdcFillEvent(),
            kafkaTemplate,
            this.compensatorProps.baseUrl(),
            restTemplate,
            objectMapper
        );
    }

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

}