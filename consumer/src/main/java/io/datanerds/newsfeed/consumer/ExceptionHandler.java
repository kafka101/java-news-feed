package io.datanerds.newsfeed.consumer;

@FunctionalInterface
public interface ExceptionHandler {
    void handle(Exception ex);
}
