package examples.kafka.example;

import akka.actor.ActorSystem;
import akka.actor.ActorSystemImpl;
import akka.kafka.ConsumerSettings;
import akka.kafka.ProducerSettings;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Source;
import examples.kafka.example.scenarios.KafkaPlainConsumerScenario;
import examples.kafka.example.scenarios.KafkaProducerScenario;
import examples.kafka.example.scenarios.ReactiveKafkaProducerScenario;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.reflections.Reflections;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Set;

public class Runner {
    private static final int LIMIT;
    private static final Container CONTAINER;

    static {
        LIMIT = 100;
        CONTAINER = new MapContainer();

        CONTAINER.set(ActorSystem.class, ActorSystem.create());
        CONTAINER.set(Consumer.class, KafkaConsumerUnit.create(Arrays.asList("example")));
        CONTAINER.set(new KafkaProducerUnit());
        CONTAINER.set(ConsumerSettings.create(CONTAINER.get(ActorSystem.class), new StringDeserializer(), new StringDeserializer())
                .withBootstrapServers("192.168.0.108:9092")
                .withGroupId("group1")
                .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
        CONTAINER.set(Materializer.class, ActorMaterializer.create(CONTAINER.get(ActorSystem.class)));
        CONTAINER.set(ProducerSettings
                .create(CONTAINER.get(ActorSystem.class), new StringSerializer(), new StringSerializer())
                .withBootstrapServers("192.168.0.108:9092"));
        CONTAINER.set(Source.class, Source.range(1, LIMIT));

        Reflections reflections = new Reflections("examples.kafka.example.scenarios");
        Set<Class<? extends Scenario>> classes = reflections.getSubTypesOf(Scenario.class);
        for (Class<? extends Scenario> aClass: classes) {
            try {
                CONTAINER.set(aClass.getConstructor(Container.class).newInstance(CONTAINER));
            } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        ActorSystem.create();
        CONTAINER.get(KafkaPlainConsumerScenario.class).run();
    }
}
