package examples.kafka.example.scenarios;

import akka.Done;
import akka.kafka.ProducerMessage;
import akka.kafka.ProducerSettings;
import akka.kafka.javadsl.Producer;
import akka.stream.Materializer;
import akka.stream.javadsl.Source;
import examples.kafka.example.Container;
import examples.kafka.example.KafkaProducerUnit;
import examples.kafka.example.Scenario;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.CompletionStage;

public class ReactiveKafkaProducerScenario extends Scenario {
    private static final int limit = 100;

    public ReactiveKafkaProducerScenario(Container container) {
        super(container);
    }

    @Override
    protected void execute() {
        Materializer materializer = container.get(Materializer.class);
        ProducerSettings<String, String> producerSettings = container.get(ProducerSettings.class);

        CompletionStage<Done> done =
                Source.range(1, 100)
                        .map(Object::toString)
                        .map(value -> new ProducerRecord<String, String>("test", value))
                        .runWith(Producer.plainSink(producerSettings), materializer);

        done.whenComplete((done1, throwable) -> System.out.println("End of producing"));
    }
}
