package examples.kafka.zookeeper.example;

import examples.kafka.zookeeper.Case;

import java.util.concurrent.BrokenBarrierException;

/**
 * Created by jdk.
 * Date: 30.04.18
 */
public class ConcurrentUpdatingCase extends ConcurrentOperationCase {
    public ConcurrentUpdatingCase() {
        super(100, "__vvv", "__sss", "/system/creating");
    }

    public static void main(String[] args)
            throws BrokenBarrierException, InterruptedException {
        new ConcurrentUpdatingCase().run();
    }

    @Override
    protected void firstAction() {
        try {
            loader.updateConfigs(firstConfigs);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void secondAction() {
        try {
            loader.updateConfigs(secondConfigs);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
