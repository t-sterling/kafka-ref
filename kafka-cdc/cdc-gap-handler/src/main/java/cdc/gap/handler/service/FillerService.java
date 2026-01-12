package cdc.gap.handler.service;

import cdc.gap.handler.domain.FillCommand;
import cdc.gap.handler.domain.FillEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.*;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Map;

@Service
public class FillerService {

    private static final Logger LOG = LoggerFactory.getLogger(FillerService.class);

    private final KafkaTemplate<String, FillEvent> kafkaTemplate;

    private final String responseTopic;
    private final String baseUrl;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    public FillerService(@Qualifier("fillEventTopic") String fillEventTopic,
                         KafkaTemplate<String, FillEvent> kafkaTemplate,
                         @Qualifier("baseUrl") String baseUrl,
                         RestTemplate restTemplate,
                         ObjectMapper objectMapper) {
        this.responseTopic = fillEventTopic;
        this.kafkaTemplate = kafkaTemplate;
        this.baseUrl = baseUrl;
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
    }

    public void processCommand(FillCommand command) throws TransientFillException {
        var fillEvent = execute(command);
        publishFillEvent(fillEvent);
    }

    private FillEvent execute(FillCommand command) throws TransientFillException {

        try {

            LOG.info("Request received for record {}", command.recordId());

            var url = buildUrl(command);
            var json = restTemplate.getForObject(url, String.class);
            var allFields = objectMapper.readValue(json, new TypeReference<Map<String, Object>>() {});
            var lastMod = Long.parseLong(allFields.get("lastModifiedEpochMs").toString());

            return new FillEvent(
                    command.recordId(),
                    command.entityType(),
                    command.eventId(),
                    lastMod,
                    allFields,
                    true,
                    ""
            );

        } catch (HttpClientErrorException | JsonProcessingException e) {

            // NON-TRANSIENT (4xx)
            return new FillEvent(
                    command.recordId(),
                    command.entityType(),
                    command.eventId(),
                    -1,
                    Map.of(),
                    false,
                    e.getMessage()
            );

        }
        catch (HttpServerErrorException e) {
            // TRANSIENT (5xx)
            throw new TransientFillException("Server error calling fill service", e);
        }
        catch (ResourceAccessException e) {
            // TRANSIENT (timeouts, connection refused)
            throw new TransientFillException("I/O error calling fill service", e);
        }
        catch (RestClientException e) {
            // Conservative default: treat as transient
            throw new TransientFillException("Unexpected RestClientException", e);
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