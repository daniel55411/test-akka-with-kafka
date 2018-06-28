package examples.kafka.example.units;

import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.ProducerSettings;
import akka.stream.Materializer;
import akka.stream.javadsl.Source;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.concurrent.CompletionStage;

public class ReactiveKafkaProducer implements ProducerUnit {
    private final ProducerSettings<String, String> producerSettings;
    private final Materializer materializer;

    public ReactiveKafkaProducer(Materializer materializer, ProducerSettings<String, String> producerSettings) {
        this.producerSettings = producerSettings;
        this.materializer = materializer;
    }

    @Override
    public void produce(String topic, List<String> data, int limit) {
        CompletionStage<Done> stage =
                Source.range(1, limit)
                        .map(index -> "reactive-" + data.get(index % Math.min(limit, data.size())) + "-" + index)
                        .map(value -> new ProducerRecord<String, String>(topic, value))
                        .runWith(akka.kafka.javadsl.Producer.plainSink(producerSettings), materializer);
    }
}
