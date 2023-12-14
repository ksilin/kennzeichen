package com.example.util;


import com.example.model.RawCarCamEventRoot;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.List;
import java.util.Map;

public class ListOfRawCarCamEventRootsSerde implements Serde<List<RawCarCamEventRoot>> {

        private final ObjectMapper objectMapper;

    public ListOfRawCarCamEventRootsSerde() {
        this(new ObjectMapper());
    }

        public ListOfRawCarCamEventRootsSerde(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            // no-op
        }

        @Override
        public void close() {
            // no-op
        }

        @Override
        public Serializer<List<RawCarCamEventRoot>> serializer() {
            return new Serializer<List<RawCarCamEventRoot>>() {
                @Override
                public void configure(Map<String, ?> configs, boolean isKey) {
                    // no-op
                }

                @Override
                public byte[] serialize(String topic, List<RawCarCamEventRoot> data) {
                    try {
                        return objectMapper.writeValueAsBytes(data);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void close() {
                    // no-op
                }
            };
        }

        @Override
        public Deserializer<List<RawCarCamEventRoot>> deserializer() {
            return new Deserializer<List<RawCarCamEventRoot>>() {
                @Override
                public void configure(Map<String, ?> configs, boolean isKey) {
                    // no-op
                }

                @Override
                public List<RawCarCamEventRoot> deserialize(String topic, byte[] data) {
                    try {
                        return objectMapper.readValue(data, new TypeReference<List<RawCarCamEventRoot>>() {});
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void close() {
                    // no-op
                }
            };
        }
}
