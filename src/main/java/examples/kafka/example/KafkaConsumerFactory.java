package examples.kafka.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class KafkaConsumerFactory {
    private static final Properties PROPERTIES;

    static {
        PROPERTIES = new Properties();

        PROPERTIES.put("bootstrap.servers", "192.168.0.108:9092");
        PROPERTIES.put("group.id", "group1");
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
        consumer.assign(topics.stream().map(s -> new TopicPartition(s, 0)).collect(Collectors.toList()));
//        consumer.subscribe(topics);

        return consumer;
    }
}