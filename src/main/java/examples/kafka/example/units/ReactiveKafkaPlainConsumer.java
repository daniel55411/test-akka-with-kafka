package examples.kafka.example.units;

import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReactiveKafkaPlainConsumer implements ConsumerUnit{
    private final Materializer materializer;
    private final ConsumerSettings<String, String> consumerSettings;

    public ReactiveKafkaPlainConsumer(Materializer materializer, ConsumerSettings<String, String> consumerSettings) {
        this.materializer = materializer;
        this.consumerSettings = consumerSettings;
    }

    @Override
    public void consume(String topic, int limit, int batchSize, Runnable whenComplete) {
        CompletionStage<Done> stage = Consumer.plainSource(consumerSettings, Subscriptions.topics(topic))
                .take(limit)
                .runWith(Sink.ignore(), materializer);

        stage.whenComplete((done, throwable) -> whenComplete.run());
    }
}
