package io.github.kafka101.newsfeed.consumer;

import org.apache.kafka.common.KafkaException;

/**
 *  Any exception during deserialization in the NewsDecoder
 *  @see io.github.kafka101.newsfeed.consumer.NewsDecoder
 */
public class DecoderException extends KafkaException {

    public DecoderException(String message, Throwable cause) {
        super(message, cause);
    }

    public DecoderException(String message) {
        super(message);
    }
}