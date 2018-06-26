package examples.kafka.example;

import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.ProducerSettings;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class LocalEnvironment implements Environment<String, String>{
    private static final ActorSystem SYSTEM = ActorSystem.create();

    private static final Materializer MATERIALIZER = ActorMaterializer.create(SYSTEM);

    private static final ConsumerSettings<String, String> CONSUMER_SETTINGS =
            ConsumerSettings.create(SYSTEM, new StringDeserializer(), new StringDeserializer())
                    .withBootstrapServers("localhost:9092")
                    .withGroupId("group1")
                    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    private static final ProducerSettings<String, String> PRODUCER_SETTINGS = ProducerSettings
            .create(SYSTEM, new StringSerializer(), new StringSerializer())
            .withBootstrapServers("localhost:9092");

    public ActorSystem getActorSystem() {
        return SYSTEM;
    }

    public Materializer getMaterializer() {
        return MATERIALIZER;
    }

    public ProducerSettings<String, String> getProducerSettings() {
        return PRODUCER_SETTINGS;
    }

    public ConsumerSettings<String, String> getConsumerSettings() {
        return CONSUMER_SETTINGS;
    }
}
