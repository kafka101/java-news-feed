package io.datanerds.newsfeed.consumer;

import io.datanerds.newsfeed.domain.News;

public interface NewsConsumer {
    /**
     * Implementations of this method should process the News and must be thread-safe.
     *
     * @param news
     */
    void consume(News news);

    /**
     * @return topic this consumer would like to subscribe to
     */
    String getTopic();
}
