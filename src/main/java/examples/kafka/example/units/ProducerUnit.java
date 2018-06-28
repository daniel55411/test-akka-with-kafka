package examples.kafka.example.units;

import java.util.List;
import java.util.function.Consumer;

public interface ProducerUnit {
    void produce(String topic, List<String> data, int limit, Consumer<Throwable> consumer);
}
