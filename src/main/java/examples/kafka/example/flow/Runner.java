package examples.kafka.example.flow;

import examples.kafka.example.ConsumerTest;
import examples.kafka.example.Environment;
import examples.kafka.example.KafkaUtils;
import examples.kafka.example.LocalEnvironment;
import examples.kafka.example.httpsource.KafkaSink;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

public class Runner {
    private static final Environment env = new LocalEnvironment();
    private static final List<String> topics = Arrays.asList("http-topic", "transformed-topic");
    private static final KafkaFlow KAFKA_FLOW = new KafkaFlow(env);
    private static final ConsumerTest consumerTest =
            new ConsumerTest(Collections.singletonList(topics.get(1)));
    private static final KafkaSink KAFKA_SINK = new KafkaSink(env);

    public static void main(String[] args) {
        produce(true);
        simpleScenario();
//        loadedScenario();
    }

    private static void simpleScenario() {
        KAFKA_FLOW.runAsync(
                new HashSet<>(Collections.singleton(topics.get(0))),
                topics.get(1),
                Handlers.DIRECT_HANDLER
        );
        consumerTest.consumeWhile(
                ConsumerTest.DEFAULT_CONSUMER_FUNCTION,
                record -> record.value().equals("попечение")
        );
    }

    private static void loadedScenario() {
        KAFKA_FLOW.runAsync(
                new HashSet<>(Collections.singleton(topics.get(0))),
                topics.get(1),
                Handlers.SLOW_DIRECT_HANDLER
        ).whenComplete(KafkaUtils.whenCompleteHandler(() -> System.out.println("Flow is end")));

        consumerTest.consumeWhile(
                ConsumerTest.DEFAULT_CONSUMER_FUNCTION,
                record -> record.value().equals("попечение")
        );
    }

    private static void produce(boolean enable) {
        if (enable) {
            KAFKA_SINK
                    .runAsync(KafkaSink.WHOLE, false)
                    .whenComplete(KafkaUtils.whenCompleteHandler(() -> System.out.println("Producing is end")));
        }
    }
}
