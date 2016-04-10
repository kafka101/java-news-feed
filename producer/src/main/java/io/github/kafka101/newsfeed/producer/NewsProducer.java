package io.github.kafka101.newsfeed.producer;

import io.github.kafka101.newsfeed.domain.News;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * This class is a simple kafka producer using the 0.9.x API. It provides a synchronous and an asynchronous method to
 * push messages to a kafka broker.
 */
public class NewsProducer {
    private final String producerName;
    private final String topic;
    private final KafkaProducer<String, News> producer;

    /**
     * @param producerName
     * @param topic
     * @param broker    {@value CommonClientConfigs#BOOSTRAP_SERVERS_DOC}
     */
    public NewsProducer(String producerName, String topic, String broker) {
        this.producerName = producerName;
        this.topic = topic;
        this.producer = createProducer(broker);
    }

    private KafkaProducer<String, News> createProducer(String broker) {
        Properties config = new Properties();
        config.put(ProducerConfig.CLIENT_ID_CONFIG, this.producerName);
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, NewsSerializer.class.getName());
        return new KafkaProducer<>(config);
    }

    public RecordMetadata send(News news) throws ExecutionException, InterruptedException {
        ProducerRecord<String, News> record = new ProducerRecord<>(topic, news.id.toString(), news);
        return this.producer.send(record).get();
    }

    public Future<RecordMetadata> sendAsync(News news) {
        ProducerRecord<String, News> record = new ProducerRecord<>(topic, news.id.toString(), news);
        return this.producer.send(record);
    }

    public void close() {
        producer.close();
    }
}
