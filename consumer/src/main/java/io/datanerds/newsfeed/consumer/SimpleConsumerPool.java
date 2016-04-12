package io.datanerds.newsfeed.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Simple consumer pool spawning consumer threads and handling failures.
 */
public class SimpleConsumerPool implements ExceptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(SimpleConsumerPool.class);
    private final List<SimpleConsumerThread> consumerThreads = new ArrayList<>();
    private final String bootstrapServers;
    private final String groupId;
    private ExecutorService pool;

    public SimpleConsumerPool(String bootstrapServers, String groupId) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
    }

    public void start(List<NewsConsumer> newsConsumers) {
        pool = Executors.newFixedThreadPool(newsConsumers.size());

        for (NewsConsumer newsConsumer : newsConsumers) {
            SimpleConsumerThread consumer = new SimpleConsumerThread(bootstrapServers, groupId, newsConsumer, this);
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
