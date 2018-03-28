package examples.kafka.example.httpsink;

import examples.kafka.example.Environment;
import examples.kafka.example.KafkaUtils;
import examples.kafka.example.LocalEnvironment;

import java.util.HashSet;
import java.util.Set;

public class Runner {
    private static final Environment env;
    private static final Set<String> topics;
    private static final KafkaSource KAFKA_SOURCE;

    static {
        env = new LocalEnvironment();

        topics = new HashSet<>();
        topics.add("http-topic");

        KAFKA_SOURCE = new KafkaSource(env);
    }

    public static void main(String[] args) {
        KAFKA_SOURCE
                .runAsync(topics)
                .thenAccept(done -> System.out.println("Connected"));
    }
}
