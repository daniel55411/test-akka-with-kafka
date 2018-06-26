package examples.kafka.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Scenario {
    private static final Logger LOGGER = LoggerFactory.getLogger(Scenario.class);

    protected final Container container;

    public Scenario(Container container) {
        this.container = container;
    }

    protected abstract void execute();

    public void run() {
        LOGGER.debug("Scenario has begun");
        execute();
        LOGGER.debug("Scenario has end");
    }
}
