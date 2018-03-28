package examples.kafka.example;

import akka.Done;

import java.util.function.BiConsumer;

public class KafkaUtils {
    public static BiConsumer<Done, Throwable> whenCompleteHandler(Runnable runnable) {
        return (done, throwable) -> {
            if (throwable != null) {
                System.out.println("Error: " + throwable.getMessage());
            }
            runnable.run();
        };
    }
}
