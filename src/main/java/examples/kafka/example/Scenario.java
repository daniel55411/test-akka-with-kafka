package examples.kafka.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class Scenario {
    private static final Logger LOGGER = LoggerFactory.getLogger(Scenario.class);

    protected abstract void execute(Map<String, Object> map);

    public void run(Map<String, Object> map) {
        LOGGER.debug("Scenario has begun");
        execute(map);
        LOGGER.debug("Scenario has end");
        System.out.println("Scenario has end");
    }
}
