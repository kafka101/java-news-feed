package io.github.kafka101.newsfeed.consumer;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static kafka.consumer.Consumer.createJavaConsumerConnector;

public class KafkaConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    private final ConsumerConnector consumerConnector;
    private final String topic;
    private ExecutorService pool;
    private final NewsConsumer consumer;

    public KafkaConsumer(String zookeeper, String groupId, NewsConsumer consumer) {
        this.consumerConnector = createJavaConsumerConnector(createConsumerConfig(zookeeper, groupId));
        this.consumer = consumer;
        this.topic = consumer.getTopic();
    }

    private ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }

    public void shutdown() {
        if (consumerConnector != null) {
            consumerConnector.shutdown();
        }
        try {
            shutdownExecutor();
        } catch (InterruptedException e) {
            logger.error("Interrupted during shutdown, exiting uncleanly {}", e);
        }
    }

    private void shutdownExecutor() throws InterruptedException {
        if (pool == null) {
            return;
        }
        pool.shutdown();
        if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
            List<Runnable> rejected = pool.shutdownNow();
            logger.debug("Rejected tasks: {}", rejected.size());
            if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                logger.error("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        }
    }

    public void run(int numThreads) {
        Map<String, Integer> topicCountMap = new HashMap();
        topicCountMap.put(topic, new Integer(numThreads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(
                topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        // create fixed size thread pool to launch all the threads
        pool = Executors.newFixedThreadPool(numThreads);

        // create consumer threads to handle the messages
        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            pool.submit(new KafkaConsumerThread(stream, threadNumber, consumer));
            threadNumber++;
        }
    }
}
