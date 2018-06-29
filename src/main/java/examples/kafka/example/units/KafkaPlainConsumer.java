package examples.kafka.example.units;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaPlainConsumer implements ConsumerUnit {
    private final Consumer<String, String> consumer;

    public KafkaPlainConsumer(Consumer<String, String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void consume(String topic, int limit, int batchSize, Runnable whenComplete) {
        int read = 0;

        while (read < limit) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            records.forEach(ConsumerRecord::value);
            read += records.count();
        }

        whenComplete.run();
    }
}
