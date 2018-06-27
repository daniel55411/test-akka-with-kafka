package examples.kafka.example.scenarios;

import akka.Done;
import akka.actor.ActorSystem;
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

        CompletionStage<Done> stage =
                Source.range(1, 100)
                        .map(Object::toString)
                        .map(value -> new ProducerRecord<String, String>("example", "reactive-" + value))
                        .map(param -> {
                            System.out.println(param.value());
                            return param;
                        })
                        .runWith(Producer.plainSink(producerSettings), materializer);

        stage.whenComplete((done, throwable) -> container.get(ActorSystem.class).terminate());
    }
}
