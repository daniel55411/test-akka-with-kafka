package examples.kafka.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerTest{
    public static final ProducerTest PRODUCER_TEST = new ProducerTest();
    private static final Properties PROPERTIES;

    static {
        PROPERTIES = new Properties();
        PROPERTIES.put("bootstrap.servers", "127.0.0.1:9092");
        PROPERTIES.put("acks", "all");
        PROPERTIES.put("retries", 0);
        PROPERTIES.put("batch.size", 16384);
        PROPERTIES.put("linger.ms", 1);
        PROPERTIES.put("buffer.memory", 33554432);
        PROPERTIES.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        PROPERTIES.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    private final KafkaProducer<String, String> producer;

    private ProducerTest() {
        producer = new KafkaProducer<>(PROPERTIES);
    }

    public void send(String topic, String message) {
        producer.send(new ProducerRecord<>(topic, message));
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        producer.close();
    }
}
