package examples.kafka.example.units;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface ConsumerUnit {
    void consume(String topic, int limit, int batchSize, Runnable whenComplete);
}
