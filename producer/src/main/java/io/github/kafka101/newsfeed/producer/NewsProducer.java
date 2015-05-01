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
import java.util.concurrent.Future;

/**
 *
 */
public class NewsProducer {
    private static final Logger logger = LoggerFactory.getLogger(NewsProducer.class);
    private final String producerName;
    private final String topic;
    private final KafkaProducer<String, byte[]> producer;
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

    private KafkaProducer<String, byte[]> createProducer(String broker) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public void send(News news) {
        try {
            byte[] value = mapper.writeValueAsBytes(news);
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, news.id.toString(), value);
            this.sendSync(record);
        } catch (JsonProcessingException | InterruptedException | ExecutionException ex) {
            logger.error("Uuupsss... something went wrong: {}", ex.getMessage(), ex);
        }
    }

    public void sendAsync(News news) {
        try {
            byte[] value = mapper.writeValueAsBytes(news);
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, news.id.toString(), value);
            this.sendAsync(record);
        } catch (JsonProcessingException ex) {
            logger.error("Uuupsss... something went wrong: {}", ex.getMessage(), ex);
        }
    }

    private Future<RecordMetadata> sendAsync(ProducerRecord<String, byte[]> record) {
        return this.producer.send(record);
    }

    private RecordMetadata sendSync(ProducerRecord<String, byte[]> record)
            throws ExecutionException, InterruptedException {
        return sendAsync(record).get();
    }

    public void close() {
        producer.close();
    }
}
