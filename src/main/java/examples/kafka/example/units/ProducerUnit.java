package examples.kafka.example.units;

import java.util.List;

public interface ProducerUnit {
    void produce(String topic, List<String> data, int limit);
}
