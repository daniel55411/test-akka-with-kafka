package examples.kafka.example;

import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.ProducerSettings;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import examples.kafka.example.scenarios.*;
import examples.kafka.example.units.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.reflections.Reflections;

import java.awt.*;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.List;
import java.util.concurrent.Semaphore;

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

        assembly("examples.kafka.example.scenarios", Scenario.class);
        CONTAINER.put("KafkaAtLeastOnceDeliveryConsumer", new KafkaAtLeastOnceDeliveryConsumer(CONSUMER));
        CONTAINER.put("KafkaPlainConsumer", new KafkaPlainConsumer(CONSUMER));
        CONTAINER.put("KProducer", new KProducer(PRODUCER));
        CONTAINER.put("ReactiveKafkaAtLeastOnceDeliveryConsumer", new ReactiveKafkaAtLeastOnceDeliveryConsumer(MATERIALIZER, CONSUMER_SETTINGS));
        CONTAINER.put("ReactiveKafkaPlainConsumer", new ReactiveKafkaPlainConsumer(MATERIALIZER, CONSUMER_SETTINGS));
        CONTAINER.put("ReactiveKafkaProducer", new ReactiveKafkaProducer(MATERIALIZER, PRODUCER_SETTINGS));
    }

    public static void main(String[] args) throws InterruptedException {
//        ((ReactiveKafkaProducerScenario)CONTAINER.get("ReactiveKafkaProducerScenario")).run(CONTAINER);
//        ((ReactiveKafkaPlainConsumerScenario)CONTAINER.get("ReactiveKafkaPlainConsumerScenario")).run(CONTAINER);
        ((ReactiveKafkaAtLeastOnceDeliveryScenario)CONTAINER.get("ReactiveKafkaAtLeastOnceDeliveryScenario")).run(CONTAINER);
        ACTOR_SYSTEM.terminate();
    }

    private static <T> void assembly(String pack, Class<T> subtype) {
        Reflections reflections = new Reflections(pack);
        Set<Class<? extends T>> classes = reflections.getSubTypesOf(subtype);
        for (Class<? extends T> aClass: classes) {
            try {
                System.out.println(aClass.getSimpleName());
                CONTAINER.put(aClass.getSimpleName(), aClass.getConstructor().newInstance());
            } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
                e.printStackTrace();
            }
        }
    }
}
