package io.github.kafka101.newsfeed.test;

import io.github.kafka101.newsfeed.consumer.KafkaConsumer;
import io.github.kafka101.newsfeed.consumer.NewsConsumer;
import io.github.kafka101.newsfeed.domain.News;
import io.github.kafka101.newsfeed.producer.NewsProducer;
import net._01001111.text.LoremIpsum;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.jayway.awaitility.Awaitility.await;

public class EndToEndTest {

    private static final Logger logger = LoggerFactory.getLogger(EndToEndTest.class);
    private static final LoremIpsum LOREM_IPSUM = new LoremIpsum();
    private static final String BROKER = "127.0.0.1:9092";
    private static final String ZOOKEEPER = "127.0.0.1:2181";
    private static final int THREADS = 1;
    private static final String TOPIC = "finance_news";

    @Test
    public void sendAndReceiveTest() throws InterruptedException {
        int messageNumber = 50;
        NewsProducer newsProducer = new NewsProducer("Financial News", TOPIC, BROKER);
        logger.info("Sending {} messages", messageNumber);
        long start = System.currentTimeMillis();
        for (int i = 0; i < messageNumber; i++) {
            News news = new News(UUID.randomUUID(), "Prof. Kohle", LOREM_IPSUM.paragraph(), LOREM_IPSUM.paragraph());
            newsProducer.sendAsync(news);
        }
        logger.info("Sent {} messages in {}ms", messageNumber, System.currentTimeMillis() - start);

        TestConsumer newsConsumer = new TestConsumer();
        KafkaConsumer kafkaConsumer = new KafkaConsumer(ZOOKEEPER, "test-group", newsConsumer);
        kafkaConsumer.run(THREADS);

        await().atMost(5, TimeUnit.SECONDS).until(() -> newsConsumer.getReceivedMessages() == messageNumber);
        kafkaConsumer.shutdown();
    }

    public class TestConsumer implements  NewsConsumer {

        AtomicInteger counter = new AtomicInteger(0);

        @Override
        public void consume(News news) {
            logger.debug("Counting: {}", counter.incrementAndGet());
        }

        @Override
        public String getTopic() {
            return TOPIC;
        }

        @Override
        public String getName() {
            return "TestConsumer";
        }

        public int getReceivedMessages() {
            return counter.get();
        }
    }
}
