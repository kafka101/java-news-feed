package io.github.kafka101.newsfeed.producer;

import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

public class UUIDSerializer implements Serializer<UUID> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // nop
    }

    @Override public byte[] serialize(String topic, UUID data) {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[16]);
        buffer.putLong(data.getMostSignificantBits());
        buffer.putLong(data.getLeastSignificantBits());
        return buffer.array();
    }

    @Override public void close() {
        // nop
    }
}
