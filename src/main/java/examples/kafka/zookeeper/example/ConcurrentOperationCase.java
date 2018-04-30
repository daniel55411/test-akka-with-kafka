package examples.kafka.zookeeper.example;

import examples.kafka.zookeeper.Case;
import examples.kafka.zookeeper.flow.ConfigLoader;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by jdk.
 * Date: 30.04.18
 */
public abstract class ConcurrentOperationCase extends Case{
    private final static String KEY = "node";

    protected List<Map.Entry<String, byte[]>> firstConfigs;
    protected List<Map.Entry<String, byte[]>> secondConfigs;
    protected ConfigLoader loader;

    public ConcurrentOperationCase(
            int count,
            String value1,
            String value2,
            String path) {
        firstConfigs = createConfig(count, value1, path);
        secondConfigs = createConfig(count, value2, path);
        loader = new ConfigLoader("");
    }

    private List<Map.Entry<String, byte[]>> createConfig(int count, String value, String path) {
        return IntStream
                .range(1, count)
                .mapToObj(i -> new AbstractMap.SimpleEntry<>(
                        path + "/" + KEY + i,
                        (value + i).getBytes())
                )
                .collect(Collectors.toList());
    }
}
