package examples.kafka.example.scenarios;

import examples.kafka.example.Scenario;
import examples.kafka.example.units.KProducer;

import java.util.Arrays;
import java.util.Map;

public class KafkaProducerScenario extends Scenario {
    private static final int LIMIT = 100;

    @Override
    protected void execute(Map<String, Object> container) {
        ((KProducer) container.get("KProducer")).produce("example", Arrays.asList("ex", "23"), LIMIT, () -> {});
    }
}
