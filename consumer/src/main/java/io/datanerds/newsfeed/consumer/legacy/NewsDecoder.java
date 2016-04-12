package io.datanerds.newsfeed.consumer.legacy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.datanerds.newsfeed.domain.News;
import kafka.serializer.Decoder;

import java.io.IOException;

public class NewsDecoder implements Decoder<News> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public News fromBytes(byte[] bytes) {
        try {
            return mapper.readValue(bytes, News.class);
        } catch (IOException e) {
            throw new DecoderException("Cannot read message.", e);
        }
    }
}
