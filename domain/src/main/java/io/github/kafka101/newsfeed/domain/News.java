package io.github.kafka101.newsfeed.domain;

public class News {
    public final String headline;
    public final String story;

    public News(String headline, String story) {
        this.headline = headline;
        this.story = story;
    }
}
