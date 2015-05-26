package io.github.kafka101.newsfeed.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.kafka101.newsfeed.domain.News;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 *
 */
public class NewsProducer {
    private static final Logger logger = LoggerFactory.getLogger(NewsProducer.class);
    private final String producerName;
    private final String topic;
    private final KafkaProducer<String, News> producer;
    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * @param producerName
     * @param topic
     * @param broker       {@value ProducerConfig#BOOSTRAP_SERVERS_DOC}
     */
    public NewsProducer(String producerName, String topic, String broker) {
        this.producerName = producerName;
        this.topic = topic;
        this.producer = createProducer(broker);
    }

    private KafkaProducer<String, News> createProducer(String broker) {
        Properties config = new Properties();
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
