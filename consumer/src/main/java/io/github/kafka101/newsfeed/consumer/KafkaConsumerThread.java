package io.github.kafka101.newsfeed.consumer;

import io.github.kafka101.newsfeed.domain.News;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerThread implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerThread.class);

    private final KafkaStream messageStream;
    private final NewsConsumer consumer;
    private final String name;

    public KafkaConsumerThread(KafkaStream messageStream, String name, NewsConsumer consumer) {
        this.messageStream = messageStream;
        this.name = name;
        this.consumer = consumer;
    }

    public void run() {
        Thread.currentThread().setName(name);
        logger.info("Started consumer thread {}", name);
        ConsumerIterator<String, News> it = messageStream.iterator();
        while (it.hasNext()) {
            relayMessage(it.next());
        }
        logger.info("Shutting down consumer thread {}", name);
    }

    private void relayMessage(MessageAndMetadata<String, News> kafkaMessage) {
        logger.trace("Received message with key '{}' and offset '{}' on partition '{}' for topic '{}'",
                kafkaMessage.key(), kafkaMessage.offset(), kafkaMessage.partition(), kafkaMessage.topic());
        consumer.consume(kafkaMessage.message());
    }
}
