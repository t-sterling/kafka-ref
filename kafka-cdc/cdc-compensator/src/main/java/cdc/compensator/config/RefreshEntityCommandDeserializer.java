package cdc.compensator.config;

import cdc.compensator.domain.FillCommand;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

/**
 * used by the kafka config
 */
public class RefreshEntityCommandDeserializer implements Deserializer<FillCommand> {

    private final ObjectMapper mapper;

    public RefreshEntityCommandDeserializer() {
        this.mapper = new ObjectMapper();
    }

    @Override
    public FillCommand deserialize(String s, byte[] bytes) {
        try {
            // Use the mapper to convert the byte array into a RefreshEntityCommand object
            return mapper.readValue(bytes, FillCommand.class);
        } catch (IOException e) {
            // Handle exception (logging or rethrowing)
            throw new RuntimeException("Failed to deserialize JSON to RefreshEntityCommand", e);
        }
    }

}