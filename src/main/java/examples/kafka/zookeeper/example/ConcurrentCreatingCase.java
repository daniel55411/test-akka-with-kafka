package examples.kafka.zookeeper.example;

import java.util.concurrent.BrokenBarrierException;

/**
 * Created by jdk.
 * Date: 30.04.18
 */
public class ConcurrentCreatingCase extends ConcurrentOperationCase {
    public ConcurrentCreatingCase() {
        super(100, "vvv", "sss", "/system/creating");
    }

    public static void main(String[] args) {
        try {
            new ConcurrentCreatingCase().run();
        } catch (BrokenBarrierException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void firstAction() {
        loader.createConfigs(firstConfigs);
    }

    @Override
    protected void secondAction() {
        loader.createConfigs(secondConfigs);
    }
}
