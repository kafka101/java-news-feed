package io.github.kafka101.newsfeed.consumer;

import io.github.kafka101.newsfeed.domain.News;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SimpleLoggingNewsConsumer implements NewsConsumer {

    private static final Logger logger = LoggerFactory.getLogger(SimpleLoggingNewsConsumer.class);
    private final String topic;

    public SimpleLoggingNewsConsumer(String topic) {
        this.topic = topic;
    }

    @Override
    public void consume(News news) {
        logger.info("Received message {}", news);
    }

    @Override
    public String getTopic() {
        return topic;
    }

}
