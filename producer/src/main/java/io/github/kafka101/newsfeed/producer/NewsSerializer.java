package io.github.kafka101.newsfeed.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.kafka101.newsfeed.domain.News;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class NewsSerializer implements Serializer<News> {

    private static final Logger logger = LoggerFactory.getLogger(NewsSerializer.class);
    ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // nop
    }

    @Override
    public byte[] serialize(String topic, News data) {
        try {
            return mapper.writeValueAsBytes(data);
        } catch (JsonProcessingException ex) {
            throw new SerializationException("Could not transform Object to JSON: " + ex.getMessage(), ex);
        }
    }

    @Override
    public void close() {
        // nop
    }
}
