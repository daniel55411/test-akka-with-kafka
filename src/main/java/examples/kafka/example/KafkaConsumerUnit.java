package examples.kafka.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.List;
import java.util.Properties;

public class KafkaConsumerUnit {
    private static final Properties PROPERTIES;

    static {
        PROPERTIES = new Properties();

        PROPERTIES.put("bootstrap.servers", "192.168.0.108:9092");
        PROPERTIES.put("group.id", "group-1");
        PROPERTIES.put("enable.auto.commit", "true");
        PROPERTIES.put("auto.commit.interval.ms", "1000");
        PROPERTIES.put("auto.offset.reset", "earliest");
        PROPERTIES.put("session.timeout.ms", "30000");
        PROPERTIES.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        PROPERTIES.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }

    public static Consumer<String, String> create() {
        return new KafkaConsumer<>(PROPERTIES);
    }

    public static Consumer<String, String> create(List<String> topics) {
        Consumer<String, String> consumer = create();
        consumer.subscribe(topics);

        return consumer;
    }
}