package examples.kafka.example.flow;

import examples.kafka.example.ConsumerTest;
import examples.kafka.example.httpsource.Requester;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

public class Profiler {
    private static final List<String> topics = Arrays.asList("http-topic", "transformed-topic");
    private static final Piper piper = new Piper();
    private static final ConsumerTest consumerTest =
            new ConsumerTest(Collections.singletonList(topics.get(1)));
    private static final Requester requester = new Requester();

    public static void main(String[] args) {
        produce(false);
//        simpleScenario();
        loadedScenario();
    }

    private static void simpleScenario() {
        piper.runAsync(
                new HashSet<>(Collections.singleton(topics.get(0))),
                topics.get(1),
                Handlers.DIRECT_HANDLER
        );
        consumerTest.consumeWhile(ConsumerTest.DEFAULT_CONSUMER_FUNCTION, record -> record.value().equals("попечение"));
    }

    private static void loadedScenario() {
        piper.runAsync(
                new HashSet<>(Collections.singleton(topics.get(0))),
                topics.get(1),
                Handlers.SLOW_DIRECT_HANDLER
        );
        consumerTest.consumeWhile(ConsumerTest.DEFAULT_CONSUMER_FUNCTION, record -> record.value().equals("попечение"));
    }

    private static void produce(boolean enable) {
        if (enable) {
            requester.runAsync(Requester.WHOLE, false, () -> System.out.print("Producing is end"));
        }
    }
}
