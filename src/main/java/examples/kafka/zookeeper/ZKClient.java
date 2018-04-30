package examples.kafka.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jdk.
 * Date: 30.04.18
 */
public class ZKClient {
    private final static Logger LOGGER = LoggerFactory.getLogger(ZKClient.class);

    public final static String DEFAULT_ZK_CONNECTION_STRING = "localhost:2181";
    public final static int DEFAULT_CONNECTION_RETRIES = 5;
    public final static int MAX_INTERVAL_BETWEEN_RETRIES = 25000;
    public final static int MIN_INTERVAL_BETWEEN_RETRIES = 250;
    public final static RetryPolicy DEFAULT_RETRY_POLICY = new BoundedExponentialBackoffRetry(
            MIN_INTERVAL_BETWEEN_RETRIES,
            MAX_INTERVAL_BETWEEN_RETRIES,
            DEFAULT_CONNECTION_RETRIES
    );

    public CuratorFramework zkClient;

    public ZKClient(String connectString, RetryPolicy retryPolicy, String namespace) {
        zkClient = CuratorFrameworkFactory
                .builder()
                .connectString(connectString)
                .retryPolicy(retryPolicy)
                .namespace(namespace)
                .build();

        zkClient.start();
    }

    public ZKClient(String namespace) {
        this(DEFAULT_ZK_CONNECTION_STRING, DEFAULT_RETRY_POLICY, namespace);
    }

    public void close() {
        zkClient.close();
    }
}
