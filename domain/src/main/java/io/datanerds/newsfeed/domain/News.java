package io.datanerds.newsfeed.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.UUID;

public class News {
    public final UUID id;
    public final String author;
    public final String title;
    public final String body;

    @JsonCreator
    public News(@JsonProperty("id") UUID id,
            @JsonProperty("author") String author,
            @JsonProperty("title") String title,
            @JsonProperty("body") String body) {
        this.id = id;
        this.author = author;
        this.title = title;
        this.body = body;
    }
}
