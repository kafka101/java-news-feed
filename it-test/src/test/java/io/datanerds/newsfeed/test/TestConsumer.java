package io.datanerds.newsfeed.test;

import io.datanerds.newsfeed.consumer.NewsConsumer;
import io.datanerds.newsfeed.domain.News;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestConsumer implements NewsConsumer {

    private static final Logger logger = LoggerFactory.getLogger(TestConsumer.class);
    private List<News> newsList = Collections.synchronizedList(new ArrayList());
    private final String topic;

    public TestConsumer(String topic) {
        this.topic = topic;
    }

    @Override
    public void consume(News news) {
        logger.debug("Adding News!");
        newsList.add(news);
    }

    @Override
    public String getTopic() {
        return topic;
    }

    public int getMessageCount() {
        return newsList.size();
    }

    public List<News> getNews() {
        return Collections.unmodifiableList(newsList);
    }
}