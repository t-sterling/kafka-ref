package cdc.gap.handler.config;

import cdc.gap.handler.domain.FillCommand;
import cdc.gap.handler.domain.FillEvent;
import cdc.gap.handler.service.KafkaBackPressureController;
import cdc.gap.handler.service.FillerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.web.client.RestTemplate;

@Configuration
@EnableKafka
public class KafkaConfig {

    private final GapHandlerProps gapHandlerProps;

    public KafkaConfig(GapHandlerProps appProps) {
        this.gapHandlerProps = appProps;
    }

    /**
     * listen to RefreshEntityCommands
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, FillCommand> kafkaListenerContainerFactory(
            ConsumerFactory<String, FillCommand> consumerFactory) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, FillCommand>();
        factory.setConsumerFactory(consumerFactory);

        // manual acks because we are treating it as a work queue and must publish before commiting
        //
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
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
    FillerService fillerService(KafkaTemplate<String, FillEvent> kafkaTemplate,
                                     RestTemplate restTemplate,
                                     ObjectMapper objectMapper,
                                     KafkaBackPressureController fillCommandPauser) {
        return new FillerService(
                this.gapHandlerProps.topics().cdcFillEvent(),
                kafkaTemplate,
                this.gapHandlerProps.baseUrl(),
                restTemplate,
                objectMapper
        );
    }

    /*@Bean
    FillCommandPauser fillCommandPauser(FillerService fillerService,
                                        KafkaListenerEndpointRegistry registry){
        return new FillCommandPauser(fillerService, registry);
    }*/

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }


    @Bean("fillEventTopic")
    String fillEventTopic(){
        return this.gapHandlerProps.topics().cdcFillEvent();
    }

    @Bean("baseUrl")
    String baseUrl(){
        return this.gapHandlerProps.baseUrl();
    }

    @Bean
    GapHandlerProps.Retries retries(){
        return this.gapHandlerProps.retries();
    }

}
