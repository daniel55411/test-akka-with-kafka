package examples.kafka.example;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.Random;

public class RandomKafkaPartitioner implements Partitioner {
    private final String topic;

    public RandomKafkaPartitioner(String topic) {
        this.topic = topic;
    }

    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        int totalParitions = cluster.partitionCountForTopic(topic);
        Random intgerGen = new Random();
        int targetPartition = intgerGen.nextInt(totalParitions-1); //partition id starts with 0
        System.out.println("Record will be placed in partition: "+ targetPartition);
        return targetPartition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
