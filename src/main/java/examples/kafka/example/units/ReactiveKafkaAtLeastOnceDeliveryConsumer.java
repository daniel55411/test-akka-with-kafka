package examples.kafka.example.units;

import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.ConsumerMessage;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReactiveKafkaAtLeastOnceDeliveryConsumer implements ConsumerUnit {
    private final Materializer materializer;
    private final ConsumerSettings<String, String> consumerSettings;

    public ReactiveKafkaAtLeastOnceDeliveryConsumer(Materializer materializer, ConsumerSettings<String, String> consumerSettings) {
        this.materializer = materializer;
        this.consumerSettings = consumerSettings;
    }

    @Override
    public void consume(String topic, int limit, int batchSize, java.util.function.Consumer<Throwable> whenComplete) {

        CompletionStage<Done> stage = Consumer.committableSource(consumerSettings, Subscriptions.topics("example"))
                .take(limit)
                .map(ConsumerMessage.CommittableMessage::committableOffset)
                .batch(batchSize, ConsumerMessage::createCommittableOffsetBatch, ConsumerMessage.CommittableOffsetBatch::updated)
                .mapAsync(3, ConsumerMessage.Committable::commitJavadsl)
                .runWith(Sink.ignore(), materializer);

        stage.whenComplete((done, throwable) -> whenComplete.accept(throwable));
    }
}
