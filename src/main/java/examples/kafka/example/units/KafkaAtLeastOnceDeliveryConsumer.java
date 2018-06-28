package examples.kafka.example.units;

import akka.actor.ActorSystem;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaAtLeastOnceDeliveryConsumer implements ConsumerUnit {
    private final org.apache.kafka.clients.consumer.Consumer<String, String> consumer;

    public KafkaAtLeastOnceDeliveryConsumer(Consumer<String, String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void consume(String topic, int limit, int batchSize, java.util.function.Consumer<Throwable> whenComplete) {
        AtomicLong accumulatedMessageCount = new AtomicLong();
        AtomicLong lastProcessedOffset = new AtomicLong();
        AtomicBoolean commitInProgress = new AtomicBoolean(false);
        AtomicLong read = new AtomicLong();

        Set<TopicPartition> assignment = consumer.assignment();
        while (read.longValue() < limit) {
            if (!commitInProgress.get()) {
                consumer.resume(assignment);
            }

            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                if (read.longValue() >= limit) {
                    break;
                }

                accumulatedMessageCount.incrementAndGet();
                read.incrementAndGet();
                lastProcessedOffset.set(record.offset());

                if (accumulatedMessageCount.longValue() >= batchSize) {
                    if (!commitInProgress.get()) {
                        commitInProgress.set(true);
                        commit(topic, accumulatedMessageCount, commitInProgress, lastProcessedOffset);
                    } else {
                        consumer.pause(assignment);
                    }
                }
            }
        }

        whenComplete.accept(null);
    }

    private void commit(String topic,
                        AtomicLong accumulatedMessageCount,
                        AtomicBoolean commitInProgress,
                        AtomicLong lastProcessedOffset) {
        accumulatedMessageCount.set(0);
        Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
        offsetMap.put(new TopicPartition(topic, 0), new OffsetAndMetadata(lastProcessedOffset.get()));

        consumer.commitAsync(offsetMap, (map, e) -> {
            commitInProgress.set(false);
        });

    }
}
