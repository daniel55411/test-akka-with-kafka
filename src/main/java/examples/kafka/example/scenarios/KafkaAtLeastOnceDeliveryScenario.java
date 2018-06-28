package examples.kafka.example.scenarios;

import examples.kafka.example.Scenario;
import examples.kafka.example.units.KafkaAtLeastOnceDeliveryConsumer;

import java.util.Map;

public class KafkaAtLeastOnceDeliveryScenario extends Scenario {
    private static final int LIMIT = 100;
    private static final int BATCH_SIZE = 10;

    @Override
    protected void execute(Map<String, Object> map) {
        ((KafkaAtLeastOnceDeliveryConsumer) map.get("KafkaAtLeastOnceDeliveryConsumer")).consume("example", LIMIT, BATCH_SIZE);
    }
}
