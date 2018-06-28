package examples.kafka.example.scenarios;

import examples.kafka.example.Scenario;
import examples.kafka.example.units.ReactiveKafkaAtLeastOnceDeliveryConsumer;

import java.util.Map;

public class ReactiveKafkaAtLeastOnceDeliveryScenario extends Scenario {
    private static final int LIMIT = 100;
    private static final int BATCH_SIZE = 10;

    @Override
    protected void execute(Map<String, Object> map) {
        ((ReactiveKafkaAtLeastOnceDeliveryConsumer)map.get("ReactiveKafkaAtLeastOnceDeliveryConsumer")).consume("example", LIMIT, BATCH_SIZE, throwable -> {});

    }
}
