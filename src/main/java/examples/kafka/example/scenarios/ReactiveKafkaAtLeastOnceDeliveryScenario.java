package examples.kafka.example.scenarios;

import akka.Done;
import akka.kafka.ConsumerMessage;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.kafka.ConsumerMessage.CommittableOffsetBatch;
import akka.kafka.javadsl.Producer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import examples.kafka.example.Container;
import examples.kafka.example.Scenario;

import java.util.concurrent.CompletionStage;

public class ReactiveKafkaAtLeastOnceDeliveryScenario extends Scenario {
    private final static long BATCH_SIZE = 100;

    public ReactiveKafkaAtLeastOnceDeliveryScenario(Container container) {
        super(container);
    }

    @Override
    protected void execute() {
        ConsumerSettings<String, String> consumerSettings = container.get(ConsumerSettings.class);
        Materializer materializer = container.get(Materializer.class);

        CompletionStage<Done> stage = Consumer.committableSource(consumerSettings, Subscriptions.topics("example"))
                .map(ConsumerMessage.CommittableMessage::committableOffset)
                .batch(BATCH_SIZE, ConsumerMessage::createCommittableOffsetBatch, CommittableOffsetBatch::updated)
                .mapAsync(3, ConsumerMessage.Committable::commitJavadsl)
                .runWith(Sink.ignore(), materializer);

        stage.whenComplete((done, throwable) -> System.out.println("Reactive at least once delivery has end"));
    }
}
