package examples.kafka.example.flow;

import akka.actor.ActorSystem;
import akka.kafka.*;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Set;
import java.util.function.Function;

public class Piper {
    private final ActorSystem system = ActorSystem.create();
    private final Materializer materializer = ActorMaterializer.create(system);
    private final ConsumerSettings<String, String> consumerSettings =
            ConsumerSettings.create(system, new StringDeserializer(), new StringDeserializer())
                    .withBootstrapServers("localhost:9092")
                    .withGroupId("group1")
                    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    private final ProducerSettings<String, String> producerSettings = ProducerSettings
            .create(system, new StringSerializer(), new StringSerializer())
            .withBootstrapServers("localhost:9092");

    public void runAsync(Set<String> src, String dst, Function<String, String> handler) {
        Consumer.committableSource(consumerSettings, Subscriptions.topics(src))
                .map(param -> new ProducerMessage.Message<String, String, ConsumerMessage.Committable>(
                                new ProducerRecord<String, String>(dst, handler.apply(param.record().value())), param.committableOffset()
                        ))
                .runWith(Producer.commitableSink(producerSettings), materializer);
    }
}
