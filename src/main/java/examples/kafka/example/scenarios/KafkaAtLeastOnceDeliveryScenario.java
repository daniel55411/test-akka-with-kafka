package examples.kafka.example.scenarios;

import examples.kafka.example.Container;
import examples.kafka.example.KafkaConsumerUnit;
import examples.kafka.example.Scenario;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaAtLeastOnceDeliveryScenario extends Scenario {
    private static final int LIMIT = 1000;
    private static final int BATCH_SIZE = 100;

    private final Consumer<String, String> consumer;

    public KafkaAtLeastOnceDeliveryScenario(Container container) {
        super(container);

        consumer = container.get(Consumer.class);
    }

    @Override
    protected void execute() {
        AtomicLong accumulatedMessageCount = new AtomicLong();
        AtomicLong lastProcessedOffset = new AtomicLong();
        AtomicBoolean commitInProgress = new AtomicBoolean(false);

        Set<TopicPartition> assignment = consumer.assignment();
        int read = 0;

        while (read < LIMIT) {
            if (!commitInProgress.get()) {
                consumer.resume(assignment);
            }

            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                accumulatedMessageCount.addAndGet(1);
                lastProcessedOffset.set(record.offset());

                if (accumulatedMessageCount.longValue() >= BATCH_SIZE) {
                    if (commitInProgress.get()) {
                        commitInProgress.set(true);
                        commit(accumulatedMessageCount, commitInProgress, lastProcessedOffset);
                    } else {
                        consumer.pause(assignment);
                    }
                }
            }

            read += records.count();
        }
    }

    private void commit(AtomicLong accumulatedMessageCount,
                        AtomicBoolean commitInProgress,
                        AtomicLong lastProcessedOffset) {
        accumulatedMessageCount.set(0);
        Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
        offsetMap.put(new TopicPartition("example", 0), new OffsetAndMetadata(lastProcessedOffset.get()));

        consumer.commitAsync(offsetMap, (map, e) -> commitInProgress.set(false));

    }
}
