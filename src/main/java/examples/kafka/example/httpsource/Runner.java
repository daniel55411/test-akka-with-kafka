package examples.kafka.example.httpsource;

import examples.kafka.example.ConsumerTest;
import examples.kafka.example.KafkaUtils;
import examples.kafka.example.LocalEnvironment;

import java.util.Arrays;

public class Runner {
    private static final KafkaSink KAFKA_SINK = new KafkaSink(new LocalEnvironment());
    private static final ConsumerTest consumerTest =
            new ConsumerTest(Arrays.asList("http-topic", "HelloKafkaTopic-"));

    public static void main(String[] args) {
        measure(KafkaSink.WHOLE, false);
        long result_2 = measure(KafkaSink.WHOLE, false);
        long result_1 = measure(KafkaSink.STREAM, false);

        System.out.println("Stream: " + result_1);
        System.out.println("Whole: " + result_2);
    }

    private static long measure(String type, boolean slowing) {
        long start = System.currentTimeMillis();
        KAFKA_SINK
                .runAsync(type, slowing)
                .whenComplete(KafkaUtils.whenCompleteHandler(() -> System.out.println(type)));

        consumerTest.consumeWhile(ConsumerTest.DEFAULT_CONSUMER_FUNCTION, record -> record.value().equals("попечение"));
        long end = System.currentTimeMillis();
        return end - start;
    }
}
