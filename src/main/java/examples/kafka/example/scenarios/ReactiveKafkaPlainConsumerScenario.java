package examples.kafka.example.scenarios;

import examples.kafka.example.Scenario;
import examples.kafka.example.units.KafkaAtLeastOnceDeliveryConsumer;
import examples.kafka.example.units.ReactiveKafkaPlainConsumer;

import java.util.Map;
import java.util.concurrent.Semaphore;

public class ReactiveKafkaPlainConsumerScenario extends Scenario {
    private static final int LIMIT = 100;

    @Override
    protected void execute(Map<String, Object> map) {
        ((ReactiveKafkaPlainConsumer) map.get("ReactiveKafkaPlainConsumer")).consume("example", LIMIT, 0, () -> {});
    }
}
