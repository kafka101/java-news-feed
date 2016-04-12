package io.github.kafka101.newsfeed.consumer;

@FunctionalInterface
public interface ExceptionHandler {
    void handle(Exception ex);
}
