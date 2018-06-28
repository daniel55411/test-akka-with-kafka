package examples.kafka.example.scenarios;

import examples.kafka.example.Scenario;
import examples.kafka.example.units.ReactiveKafkaProducer;

import java.util.Arrays;
import java.util.Map;

public class ReactiveKafkaProducerScenario extends Scenario {
    private static final int LIMIT = 100;

    @Override
    protected void execute(Map<String, Object> map) {
        ((ReactiveKafkaProducer) map.get("ReactiveKafkaProducer")).produce("example", Arrays.asList("123", "12312", "asdasd"), LIMIT, throwable -> {});
    }
}
