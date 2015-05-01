package io.github.kafka101.newsfeed.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.kafka101.newsfeed.domain.News;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewsProducer {
    private static final Logger logger = LoggerFactory.getLogger(NewsProducer.class);
    private static final String NEWS_TOPIC = "news_topic";
    private final KafkaProducer<String, byte[]> producer;
    private final ObjectMapper mapper = new ObjectMapper();

    public NewsProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        this.producer = new KafkaProducer<String, byte[]>(props);
    }

    public void sendNews(News news) {
        try {
            byte[] value = mapper.writeValueAsBytes(news);
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(NEWS_TOPIC, value);
            this.sendSync(record);
        } catch (JsonProcessingException | InterruptedException | ExecutionException ex) {
            logger.error("Uuupsss... something went wrong: {}", ex.getMessage(), ex);
        }
    }

    public void sendNews(String key, News news) {
        try {
            byte[] value = mapper.writeValueAsBytes(news);
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(NEWS_TOPIC, key, value);
            this.sendSync(record);
        } catch (JsonProcessingException | InterruptedException | ExecutionException ex) {
            logger.error("Uuupsss... something went wrong: {}", ex.getMessage(), ex);
        }
    }

    private void sendAsync(ProducerRecord<String, byte[]> record) {
        this.producer.send(record);
    }

    private RecordMetadata sendSync(ProducerRecord<String, byte[]> record)
            throws ExecutionException, InterruptedException {
        return this.producer.send(record).get();
    }

    public void close() {
        producer.close();
    }
}
