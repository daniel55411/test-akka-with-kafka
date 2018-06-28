package examples.kafka.example.units;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;

public class KProducer implements ProducerUnit {

    private final Producer<String, String> kafkaProducer;

    public KProducer(Producer<String, String> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public void produce(String topic, List<String> data, int limit) {
        for (int i = 0; i < limit; i++) {
            kafkaProducer.send(new ProducerRecord<>(topic, "message-" + data.get(i % Math.min(limit, data.size())) + "-" + i));
        }
    }
}
