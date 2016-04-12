package io.datanerds.newsfeed.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.datanerds.newsfeed.domain.News;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class NewsDeserializer implements Deserializer<News> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public News deserialize(String topic, byte[] data) {
        try {
            return mapper.readValue(data, News.class);
        } catch (IOException e) {
            throw new KafkaException("Cannot read message.", e);
        }
    }

    @Override
    public void close() {

    }
}
