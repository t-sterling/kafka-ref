package cdc.compensator.service;

import cdc.compensator.domain.FillEvent;
import cdc.compensator.domain.FillCommand;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Map;
import java.util.Optional;

public class CompensatorService {

    private static final Logger LOG = LoggerFactory.getLogger(CompensatorService.class);

    private final KafkaTemplate<String, FillEvent> kafkaTemplate;

    private final String responseTopic;
    private final String baseUrl;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    public CompensatorService(String responseTopic,
                              KafkaTemplate<String, FillEvent> kafkaTemplate,
                              String baseUrl,
                              RestTemplate restTemplate,
                              ObjectMapper objectMapper) {
        this.responseTopic = responseTopic;
        this.kafkaTemplate = kafkaTemplate;
        this.baseUrl = baseUrl;
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "#{'${compensator.topics.cdc-fill-command}'}", groupId = "my-group")
    public void consumeMessage(@Payload FillCommand command) {

        process(command).ifPresent(this::publishFillEvent);

    }

    private Optional<FillEvent> process(FillCommand command) {

        try {

            LOG.info("Request received for record {}", command.recordId());

            var url = buildUrl(command);
            var json = restTemplate.getForObject(url, String.class);
            var allFields = objectMapper.readValue(json, new TypeReference<Map<String, Object>>() {});
            var lastMod = Long.parseLong(allFields.get("lastModifiedEpochMs").toString());

            return Optional.of(
                new FillEvent(
                    command.recordId(),
                    command.entityType(),
                    lastMod,
                    allFields
                )
            );

        } catch (Exception e) {

            // TODO: error topic etc etc
            LOG.error("Error processing request for record {}", command.recordId(), e);
            return Optional.empty();

        }

    }

    private String buildUrl(FillCommand command){
        return UriComponentsBuilder
                .fromHttpUrl(this.baseUrl + "/{entityType}/{recordId}")
                .buildAndExpand(Map.of(
                        "entityType", command.entityType(),
                        "recordId", command.recordId()
                ))
                .toUriString();
    }

    private void publishFillEvent(FillEvent refreshedEvent) {
        var future = this.kafkaTemplate.send(responseTopic, refreshedEvent.recordId(), refreshedEvent);
        future.whenComplete((recordMetadata, exception) -> {
            if(exception != null){
                LOG.error("Error sending response for record {}", refreshedEvent.recordId(), exception);
            }
        });
    }

}