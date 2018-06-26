package examples.kafka.example.scenarios;

import examples.kafka.example.Container;
import examples.kafka.example.KafkaConsumerUnit;
import examples.kafka.example.Scenario;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class KafkaPlainConsumerScenario extends Scenario {
    private final int limit = 100;

    public KafkaPlainConsumerScenario(Container container) {
        super(container);
    }

    @Override
    protected void execute() {
        Consumer<String, String> consumer = container.get(Consumer.class);
        int read = 0;

        while (read < limit) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            records.iterator().forEachRemaining(record -> System.out.println(record.value()));
            read += records.count();
        }
    }
}
