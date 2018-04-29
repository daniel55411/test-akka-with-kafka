package examples.kafka.zookeeper.example.flow;

import examples.kafka.zookeeper.example.ZKClient;
import org.apache.curator.RetryPolicy;

/**
 * Created by jdk.
 * Date: 30.04.18
 */
public class SettingsReader extends ZKClient {

    public SettingsReader(String connectString, RetryPolicy retryPolicy, String namespace) {
        super(connectString, retryPolicy, namespace);
    }

    public SettingsReader(String namespace) {
        super(namespace);
    }

    public byte[] getData(String path) throws Exception {
        return zkClient.getData().forPath(path);
    }
}
