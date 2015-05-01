package io.github.kafka101.newsfeed.domain;

import java.util.UUID;

public class News {
    public final UUID id;
    public final String author;
    public final String title;
    public final String body;

    public News(UUID id, String author, String title, String body) {
        this.id = id;
        this.author = author;
        this.title = title;
        this.body = body;
    }
}
