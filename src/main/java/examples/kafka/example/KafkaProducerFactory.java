package examples.kafka.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerFactory {
    private static final Properties PROPERTIES;

    static {
        PROPERTIES = new Properties();
        PROPERTIES.put("bootstrap.servers", "192.168.0.108:9092");
        PROPERTIES.put("acks", "all");
        PROPERTIES.put("retries", 0);
        PROPERTIES.put("batch.size", 16384);
        PROPERTIES.put("linger.ms", 1);
        PROPERTIES.put("buffer.memory", 33554432);
        PROPERTIES.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        PROPERTIES.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    public static KafkaProducer<String, String> create() {
        return new KafkaProducer<String, String>(PROPERTIES);
    }
}
