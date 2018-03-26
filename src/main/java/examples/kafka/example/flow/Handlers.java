package examples.kafka.example.flow;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Random;
import java.util.function.Function;

public class Handlers {
    private final static Random RANDOM = new Random();
    private final static int BOUND = 100000;
    private static int delay = 1000;

    public final static Function<String, String> DIRECT_HANDLER = string -> string;
    public final static Function<String, String> JAM_HANDLER = string -> RandomStringUtils.random(RANDOM.nextInt(BOUND));
    public final static Function<String, String> LONG_STRING_HANDLER = string -> StringUtils.repeat(string, 100);

    public final static Function<String, String> SLOW_DIRECT_HANDLER = slowDown(DIRECT_HANDLER);
    public final static Function<String, String> SLOW_JAM_HANDLER = slowDown(JAM_HANDLER);
    public final static Function<String, String> SLOW_LONG_STRING_HANDLER = slowDown(LONG_STRING_HANDLER);

    public static Function<String, String> slowDown(Function<String, String> handler){
        return string -> {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return handler.apply(string);
        };
    }

    public static int getDelay() {
        return delay;
    }

    public static void setDelay(int delay) {
        Handlers.delay = delay;
    }
}
