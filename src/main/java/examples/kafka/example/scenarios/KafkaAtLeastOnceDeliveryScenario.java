package examples.kafka.example.scenarios;

import akka.actor.ActorSystem;
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
    private static final int LIMIT = 100;
    private static final int BATCH_SIZE = 10;

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
        AtomicLong read = new AtomicLong();

        Set<TopicPartition> assignment = consumer.assignment();
        while (read.longValue() < LIMIT) {
            if (!commitInProgress.get()) {
                consumer.resume(assignment);
            }

            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
                if (read.longValue() >= LIMIT) {
                    break;
                }

                accumulatedMessageCount.incrementAndGet();
                read.incrementAndGet();
                lastProcessedOffset.set(record.offset());

                if (accumulatedMessageCount.longValue() >= BATCH_SIZE) {
                    if (!commitInProgress.get()) {
                        commitInProgress.set(true);
                        commit(accumulatedMessageCount, commitInProgress, lastProcessedOffset);
                    } else {
                        consumer.pause(assignment);
                    }
                }
            }
        }

        container.get(ActorSystem.class).terminate();
    }

    private void commit(AtomicLong accumulatedMessageCount,
                        AtomicBoolean commitInProgress,
                        AtomicLong lastProcessedOffset) {
        accumulatedMessageCount.set(0);
        Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
        offsetMap.put(new TopicPartition("example", 0), new OffsetAndMetadata(lastProcessedOffset.get()));

        consumer.commitAsync(offsetMap, (map, e) -> {
            System.out.println("Committing");
            commitInProgress.set(false);
        });

    }
}
