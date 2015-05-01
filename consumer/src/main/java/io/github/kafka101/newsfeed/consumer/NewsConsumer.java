package io.github.kafka101.newsfeed.consumer;

import io.github.kafka101.newsfeed.domain.News;

public interface NewsConsumer {
    /**
     * Implementations of this method should process the News and must be thread-safe.
     * @param news
     */
    public void consume(News news);

    /**
     * @return topic this consumer would like to subscribe to
     */
    public String getTopic();
    public String getName();
}
