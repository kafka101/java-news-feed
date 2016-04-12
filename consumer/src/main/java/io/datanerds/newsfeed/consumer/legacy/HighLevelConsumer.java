package io.datanerds.newsfeed.consumer.legacy;

import io.datanerds.newsfeed.consumer.NewsConsumer;
import io.datanerds.newsfeed.domain.News;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
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

/**
 * Consumer group example for legacy consumer API using the High Level Consumer and auto-committing.
 */
public class HighLevelConsumer {

    private static final Logger logger = LoggerFactory.getLogger(HighLevelConsumer.class);

    private final ConsumerConnector consumerConnector;
    private final String topic;
    private ExecutorService pool;
    private final NewsConsumer consumer;

    public HighLevelConsumer(String zookeeper, String groupId, NewsConsumer consumer) {
        this.consumerConnector = createJavaConsumerConnector(createConsumerConfig(zookeeper, groupId));
        this.consumer = consumer;
        this.topic = consumer.getTopic();
    }

    private ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.put("offsets.storage", "kafka");
        props.put("dual.commit.enabled", "false");
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }

    public void stop() {
        if (consumerConnector != null) {
            consumerConnector.shutdown();
        }
        try {
            shutdownExecutor();
        } catch (InterruptedException e) {
            logger.error("Interrupted during stop, exiting uncleanly {}", e);
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

    public void start(int numThreads) {
        Map<String, Integer> topicCountMap = new HashMap();
        topicCountMap.put(topic, new Integer(numThreads));

        Map<String, List<KafkaStream<String, News>>> consumerMap = consumerConnector.createMessageStreams(
                topicCountMap, new StringDecoder(null), new NewsDecoder());

        List<KafkaStream<String, News>> streams = consumerMap.get(topic);

        // create fixed size thread pool to launch all the threads
        pool = Executors.newFixedThreadPool(numThreads);

        // create consumer threads to handle the messages
        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            String name = String.format("%s[%s]", consumer.getTopic(), threadNumber++);
            pool.submit(() -> processMessageStream(stream, name));
        }
    }

    private void processMessageStream(KafkaStream messageStream, String name) {
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
