package io.github.kafka101.newsfeed.producer;

import io.github.kafka101.newsfeed.domain.News;
import org.junit.Test;

public class NewsProducerTest {

    @Test
    public void testSendSync() throws Exception {
        NewsProducer newsProducer = new NewsProducer();
        News news = new News("Title", "asdf");
        //newsProducer.sendNews(news);
    }
}