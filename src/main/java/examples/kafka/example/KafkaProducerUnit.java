package examples.kafka.example;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerUnit {
    private static final Properties PROPERTIES;

    static {
        PROPERTIES = new Properties();
        PROPERTIES.put("bootstrap.servers", "192.168.0.108:9092");
        PROPERTIES.put("acks", "all");
        PROPERTIES.put("retries", 0);
        PROPERTIES.put("batch.size", 16384);
        PROPERTIES.put("linger.ms", 1);
        PROPERTIES.put("buffer.memory", 33554432);
        PROPERTIES.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        PROPERTIES.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    private final org.apache.kafka.clients.producer.KafkaProducer producer;

    public KafkaProducerUnit() {
        producer = new org.apache.kafka.clients.producer.KafkaProducer(PROPERTIES);
    }

    public void send(String topic, String message) {
        producer.send(new ProducerRecord<String, String>(topic, message));
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        producer.close();
    }
}
