package examples.kafka.example.units;

import java.util.List;

public interface ConsumerUnit {
    void consume(String topic, int limit, int batchSize);
}
