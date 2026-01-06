package com.processor.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Serdes;

/**
 * Creates a Kafka Streams Serde<T> that serializes/deserializes JSON using Jackson.
 *
 * Why this exists:
 * - Kafka Streams needs Serdes for keys/values in topics and state stores.
 * - You want JSON wire format (human readable; no Avro/Protobuf).
 */
public final class JsonSerdes {

    public static <T> Serde<T> jsonSerde(ObjectMapper om, Class<T> clazz) {
        Serializer<T> ser = (topic, data) -> {
            try { return om.writeValueAsBytes(data); }
            catch (Exception e) { throw new RuntimeException(e); }
        };

        Deserializer<T> deser = (topic, bytes) -> {
            if (bytes == null) return null;
            try { return om.readValue(bytes, clazz); }
            catch (Exception e) { throw new RuntimeException(e); }
        };

        return Serdes.serdeFrom(ser, deser);
    }

    private JsonSerdes() {}
}
