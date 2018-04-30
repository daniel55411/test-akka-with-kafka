package examples.kafka.zookeeper;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by jdk.
 * Date: 30.04.18
 */
public abstract class Case{
    protected abstract void firstAction();
    protected abstract void secondAction();

    public void run() throws BrokenBarrierException, InterruptedException {
        CyclicBarrier gate = new CyclicBarrier(3);

        Thread firstThread = new Thread(() -> runWithGate(this::firstAction, gate));
        Thread secondThread = new Thread(() -> runWithGate(this::secondAction, gate));

        firstThread.start();
        secondThread.start();

        gate.await();
    }

    private void runWithGate(Runnable runnable, CyclicBarrier gate) {
        try {
            gate.await();
            runnable.run();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
    }
}
