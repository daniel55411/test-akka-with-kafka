package examples.kafka.example.scenarios;

import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscription;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import examples.kafka.example.Container;
import examples.kafka.example.Scenario;

import java.util.concurrent.CompletionStage;

public class ReactiveKafkaPlainConsumerScenario extends Scenario {
    public ReactiveKafkaPlainConsumerScenario(Container container) {
        super(container);
    }

    @Override
    protected void execute() {
        ConsumerSettings<String, String> consumerSettings = container.get(ConsumerSettings.class);
        Materializer materializer = container.get(Materializer.class);

        CompletionStage<Done> stage = Consumer.plainSource(consumerSettings, Subscriptions.topics("example"))
                .map(record -> {
                    System.out.println(record.value());
                    return record;
                })
                .runWith(Sink.ignore(), materializer);

        stage.whenComplete((done, throwable) -> container.get(ActorSystem.class).terminate());

    }
}
