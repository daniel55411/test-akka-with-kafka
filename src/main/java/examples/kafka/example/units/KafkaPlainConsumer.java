package examples.kafka.example.units;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class KafkaPlainConsumer implements ConsumerUnit {
    private final Consumer<String, String> consumer;

    public KafkaPlainConsumer(Consumer<String, String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void consume(String topic, int limit, int batchSize) {
        int read = 0;

        while (read < limit) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            read += records.count();
        }
    }
}
