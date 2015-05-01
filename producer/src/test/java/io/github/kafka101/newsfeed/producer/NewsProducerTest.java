package io.github.kafka101.newsfeed.producer;

import io.github.kafka101.newsfeed.domain.News;
import net._01001111.text.LoremIpsum;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class NewsProducerTest {

    private static final Logger logger = LoggerFactory.getLogger(NewsProducerTest.class);
    private static final LoremIpsum LOREM_IPSUM = new LoremIpsum();
    private static final String BROKER = "127.0.0.1:9092";
    private static NewsProducer sportProducer;
    private static NewsProducer businessProducer;

    @BeforeClass
    public static void createNewsProducer() {
        sportProducer = new NewsProducer("Sport News", "sport_news", BROKER);
        businessProducer = new NewsProducer("Business News", "business_news", BROKER);
    }

    @Test
    public void sendSportNews() throws Exception {
        News news = new News(UUID.randomUUID(), "Marcel Tau", "Bayern München - Deutscher Meister",
                LOREM_IPSUM.paragraph());
        sportProducer.send(news);
    }

    @Test
    public void sendSportNewsAsync() throws Exception {
        News news = new News(UUID.randomUUID(), "Marcel Tau", "BVB schlägt Bayern München", LOREM_IPSUM.paragraph());
        sportProducer.sendAsync(news);
    }

    @Test
    public void sendBusinessNews() throws Exception {
        logger.info("Sending 1000 messages");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            News news = new News(UUID.randomUUID(), "Dr. Rainer Zufall", LOREM_IPSUM.paragraph(),
                    LOREM_IPSUM.paragraph());
            businessProducer.send(news);
        }
        logger.info("Sent 1000 msgs in {}ms", System.currentTimeMillis() - start);
    }

    @Test
    public void sendBusinessNewsAsync() throws Exception {
        logger.info("Sending 1000 messages - async");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            News news = new News(UUID.randomUUID(), "Dr. Rainer Zufall", LOREM_IPSUM.paragraph(),
                    LOREM_IPSUM.paragraph());
            businessProducer.sendAsync(news);
        }
        logger.info("Sent 1000 msgs in {}ms", System.currentTimeMillis() - start);
    }
}