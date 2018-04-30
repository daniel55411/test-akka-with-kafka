package examples.kafka.zookeeper.flow;

import examples.kafka.zookeeper.ZKClient;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    public void createConfigs(List<Map.Entry<String, byte[]>> configs) {
        configs.forEach(entry -> {
            try {
                zkClient.create()
                        .creatingParentContainersIfNeeded()
                        .forPath(entry.getKey(), entry.getValue());
            } catch (Exception e) {
                LOGGER.error(e.getMessage());
            }
        });
    }

    public void updateConfigs(List<Map.Entry<String, byte[]>> configs) throws Exception {
        List<CuratorOp> ops = configs
                .stream()
                .map(entry -> {
                    try {
                        return zkClient
                                .transactionOp()
                                .setData()
                                .forPath(entry.getKey(), entry.getValue());
                    } catch (Exception e) {
                        e.printStackTrace();
                        return null;
                    }
                })
                .collect(Collectors.toList());

        zkClient.transaction().forOperations(ops);

    }

}
