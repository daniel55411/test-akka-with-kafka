package examples.kafka.example.httpsource;

import examples.kafka.example.ConsumerTest;

import java.util.Arrays;

public class Profiler {
    private static final Requester requester = new Requester();
    private static final ConsumerTest consumerTest =
            new ConsumerTest(Arrays.asList("http-topic", "HelloKafkaTopic-"));

    public static void main(String[] args) {
        measure(Requester.WHOLE, false);
        long result_2 = measure(Requester.WHOLE, false);
        long result_1 = measure(Requester.STREAM, false);

        System.out.println("Stream: " + result_1);
        System.out.println("Whole: " + result_2);
    }

    private static long measure(String type, boolean slowing) {
        long start = System.currentTimeMillis();
        requester.runAsync(type, slowing, () -> System.out.println(type));
        consumerTest.consumeWhile(ConsumerTest.DEFAULT_CONSUMER_FUNCTION, record -> record.value().equals("попечение"));
        long end = System.currentTimeMillis();
        return end - start;
    }
}
