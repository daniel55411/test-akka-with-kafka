package examples.kafka.example.scenarios;

import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.ConsumerMessage;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.Materializer;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import examples.kafka.example.Container;
import examples.kafka.example.Scenario;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class ReactiveKafkaAtLeastOnceDeliveryScenario extends Scenario {
    private static final int LIMIT = 100;
    private static final int BATCH_SIZE = 10;

    public ReactiveKafkaAtLeastOnceDeliveryScenario(Container container) {
        super(container);
    }

    @Override
    protected void execute() {
        ConsumerSettings<String, String> consumerSettings = container.get(ConsumerSettings.class);
        Materializer materializer = container.get(Materializer.class);

        CompletionStage<Done> stage = Consumer.committableSource(consumerSettings, Subscriptions.topics("example"))
                .map(param -> {
                    System.out.println(param.record().value());
                    return param;
                })
                .take(LIMIT)
                .map(ConsumerMessage.CommittableMessage::committableOffset)
                .batch(BATCH_SIZE, ConsumerMessage::createCommittableOffsetBatch, ConsumerMessage.CommittableOffsetBatch::updated)
                .mapAsync(3, ConsumerMessage.Committable::commitJavadsl)
                .runWith(Sink.ignore(), materializer);

        stage.whenComplete((done, throwable) -> {
            System.out.println("Reactive ending");
            container.get(ActorSystem.class).terminate();
        });

    }

    private CompletionStage<String> business(String key, String value) {
        return CompletableFuture.supplyAsync(() -> {
            System.out.println(value);
            return value;
        });
    }
}
