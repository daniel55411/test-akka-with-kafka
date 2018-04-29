package examples.kafka.zookeeper.example.flow;

import com.typesafe.config.ConfigValue;
import examples.kafka.zookeeper.example.ZKClient;
import org.apache.curator.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by jdk.
 * Date: 29.04.18
 */
public class ConfigLoader extends ZKClient {
    private final static Logger LOGGER = LoggerFactory.getLogger(ConfigLoader.class);

    public ConfigLoader(String connectString, RetryPolicy retryPolicy, String namespace) {
        super(connectString, retryPolicy, namespace);
    }

    public ConfigLoader(String namespace) {
        super(namespace);
    }

    public void loadConfig(List<Map.Entry<String, ConfigValue>> configs) {
        configs.forEach(entry -> {
            try {
                zkClient.create()
                        .creatingParentContainersIfNeeded()
                        .forPath(entry.getKey(), entry.getValue().unwrapped().toString().getBytes());
            } catch (Exception e) {
                LOGGER.error(e.getMessage());
            }
        });
    }

}
