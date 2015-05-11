package io.github.kafka101.newsfeed.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.kafka101.newsfeed.domain.News;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class KafkaConsumerThread implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerThread.class);
    private final ObjectMapper mapper = new ObjectMapper();

    private final KafkaStream messageStream;
    private final int threadNumber;
    private final NewsConsumer consumer;

    public KafkaConsumerThread(KafkaStream messageStream, int threadNumber, NewsConsumer consumer) {
        this.threadNumber = threadNumber;
        this.messageStream = messageStream;
        this.consumer = consumer;
    }

    public void run() {
        logger.info("Running consumer thread #{} for {} on topic {}", threadNumber, consumer.getClass().getSimpleName(),
                consumer.getTopic());
        ConsumerIterator<String, byte[]> it = messageStream.iterator();
        while (it.hasNext()) {
            relayMessage(it.next().message());
        }
        logger.info("Shutting down consumer thread #{} for {}", threadNumber, consumer.getClass().getSimpleName());
    }

    private void relayMessage(byte[] message) {
        try {
            consumer.consume(mapper.readValue(message, News.class));
        } catch (IOException ex) {
            logger.error("Thread #{} for {}: Cannot read messages {}", threadNumber,
                    consumer.getClass().getSimpleName(), ex);
        }
    }
}
