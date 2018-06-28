package examples.kafka.example;

import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.ProducerSettings;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import examples.kafka.example.scenarios.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.reflections.Reflections;

import java.awt.*;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

public class Runner {
    private static final int LIMIT;
    private static final Map<String, Object> CONTAINER;
    private static final Consumer<String, String> CONSUMER = KafkaConsumerFactory.create(Collections.singletonList("example"));
    private static final Producer<String, String> PRODUCER = KafkaProducerFactory.create();
    private static final ActorSystem ACTOR_SYSTEM = ActorSystem.create();
    private static final Materializer MATERIALIZER = ActorMaterializer.create(ACTOR_SYSTEM);
    private static final ConsumerSettings<String, String> CONSUMER_SETTINGS =
            ConsumerSettings.create(ACTOR_SYSTEM, new StringDeserializer(), new StringDeserializer())
                    .withBootstrapServers("192.168.0.108:9092")
                    .withGroupId("group1")
                    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    private static final ProducerSettings<String, String> PRODUCER_SETTINGS = ProducerSettings
            .create(ACTOR_SYSTEM, new StringSerializer(), new StringSerializer())
            .withBootstrapServers("192.168.0.108:9092");


    static {
        LIMIT = 1000;
        CONTAINER = new HashMap<>();

        Reflections reflections = new Reflections("examples.kafka.example.scenarios");
        Set<Class<? extends Scenario>> classes = reflections.getSubTypesOf(Scenario.class);
        for (Class<? extends Scenario> aClass: classes) {
            try {
                System.out.println(aClass.getSimpleName());
                CONTAINER.put(aClass.getSimpleName(), aClass.getConstructor().newInstance());
            } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
                e.printStackTrace();
            }
        }

        Reflections reflections = new Reflections("examples.kafka.example.units");
        Set<Class<? extends Scenario>> classes = reflections.getSubTypesOf(Scenario.class);
        for (Class<? extends Scenario> aClass: classes) {
            try {
                System.out.println(aClass.getSimpleName());
                CONTAINER.put(aClass.getSimpleName(), aClass.getConstructor().newInstance());
            } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        ((ReactiveKafkaProducerScenario)CONTAINER.get("ReactiveKafkaProducerScenario")).run(CONTAINER);
        ((ReactiveKafkaPlainConsumerScenario)CONTAINER.get("ReactiveKafkaPlainConsumerScenario")).run(CONTAINER);
    }

    private static void assembly(String pack, Classsubtype) {
        Reflections reflections = new Reflections(pack);
        Set<Class<? extends Scenario>> classes = reflections.getSubTypesOf(subtype);
        for (Class<? extends Scenario> aClass: classes) {
            try {
                System.out.println(aClass.getSimpleName());
                CONTAINER.put(aClass.getSimpleName(), aClass.getConstructor().newInstance());
            } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
                e.printStackTrace();
            }
        }
    }
}
