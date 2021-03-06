package examples.kafka.example.scenarios;

import examples.kafka.example.Scenario;
import examples.kafka.example.units.KafkaPlainConsumer;

import java.util.Map;

public class KafkaPlainConsumerScenario extends Scenario {
    private static final int LIMIT = 100;

    @Override
    protected void execute(Map<String, Object> map) {
        ((KafkaPlainConsumer) map.get("KafkaPlainConsumer")).consume("example", LIMIT, 0, () -> {});
    }
}
