package examples.kafka.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class ConsumerTest {
    public static final Consumer<ConsumerRecord<String, String>> DEFAULT_CONSUMER_FUNCTION;
    private static final Properties PROPERTIES;

    static {
        PROPERTIES = new Properties();

        PROPERTIES.put("bootstrap.servers", "127.0.0.1:9092");
        PROPERTIES.put("group.id", "group-1");
        PROPERTIES.put("enable.auto.commit", "true");
        PROPERTIES.put("auto.commit.interval.ms", "1000");
        PROPERTIES.put("auto.offset.reset", "earliest");
        PROPERTIES.put("session.timeout.ms", "30000");
        PROPERTIES.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        PROPERTIES.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        DEFAULT_CONSUMER_FUNCTION = record -> System.out.println(
                String.format("Topic %s - Partition %d - Offset %d - Value %s",
                        record.topic(), record.partition(), record.offset(), record.value())
        );
    }

    private final KafkaConsumer<String, String> consumer;

    public ConsumerTest(List<String> topics) {
        consumer = new KafkaConsumer<>(PROPERTIES);
        consumer.subscribe(topics);
    }

    public void consumeWhile(Consumer<ConsumerRecord<String, String>> consumerFunction,
                             Predicate<ConsumerRecord<String, String>> predicate) {
        boolean processing = true;
        while (processing) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                if (predicate.test(record)) processing = false;
                consumerFunction.accept(record);
            }
        }
    }

}