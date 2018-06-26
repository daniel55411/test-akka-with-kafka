package examples.kafka.example.scenarios;

import examples.kafka.example.Container;
import examples.kafka.example.KafkaProducerUnit;
import examples.kafka.example.Scenario;

public class KafkaProducerScenario extends Scenario {
    private final int limit = 100;

    public KafkaProducerScenario(Container container) {
        super(container);
    }

    @Override
    protected void execute() {
        KafkaProducerUnit unit = container.get(KafkaProducerUnit.class);

        for (int i = 0; i < limit; i++) {
            System.out.println("message-" + i);
            unit.send("example", "message-" + i);
        }
    }
}
