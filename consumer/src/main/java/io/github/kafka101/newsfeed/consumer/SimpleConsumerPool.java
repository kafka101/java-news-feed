package io.github.kafka101.newsfeed.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Simple consumer for subscribe pattern with auto-commit feature turned on.
 */
public class SimpleConsumerPool implements ExceptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(SimpleConsumerPool.class);
    private final String bootstrapServers;
    private ExecutorService pool;
    private final List<SimpleConsumerThread> consumerThreads = new ArrayList<>();

    public SimpleConsumerPool(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public void start(List<NewsConsumer> newsConsumers) {
        pool = Executors.newFixedThreadPool(newsConsumers.size());

        for (NewsConsumer newsConsumer : newsConsumers) {
            SimpleConsumerThread consumer = new SimpleConsumerThread(bootstrapServers, "test_group", newsConsumer, this);
            consumerThreads.add(consumer);
            pool.submit(consumer);
        }
    }

    public void stop() {
        consumerThreads.forEach(SimpleConsumerThread::stop);
        if (pool != null) {
            pool.shutdown();
        }
        logger.info("Shutdown all {} threads of consumer pool", consumerThreads.size());
        consumerThreads.clear();
    }

    @Override
    public void handle(Exception ex) {
        logger.error("Stopping all consumer threads due to unexpected exception.", ex);
        stop();
    }
}
