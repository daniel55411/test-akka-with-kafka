package examples.kafka.example;

import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.ProducerSettings;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import examples.kafka.example.units.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.openjdk.jmh.annotations.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Semaphore;

public class Benchmarks {
    @State(Scope.Thread)
    public static class BenchState {
        private static final Random RANDOM = new Random();

        static final String TOPIC = "bench-1";
        static final List<String> DATA = Arrays.asList("data-1", "data-2", "data-3", "data-4");

        private Consumer<String, String> CONSUMER;
        private Producer<String, String> PRODUCER;
        private ActorSystem ACTOR_SYSTEM;
        private  Materializer MATERIALIZER;
        private ConsumerSettings<String, String> CONSUMER_SETTINGS;
        private ProducerSettings<String, String> PRODUCER_SETTINGS;

        Semaphore semaphore;
        Runnable whenComplete;
        KafkaPlainConsumer kafkaPlainConsumer;
        ReactiveKafkaPlainConsumer reactiveKafkaPlainConsumer;
        KafkaAtLeastOnceDeliveryConsumer kafkaAtLeastOnceDeliveryConsumer;
        ReactiveKafkaAtLeastOnceDeliveryConsumer reactiveKafkaAtLeastOnceDeliveryConsumer;
        KProducer kProducer;
        ReactiveKafkaProducer reactiveKafkaProducer;

        @Param({ "10000" })
        public int limit;


        @Setup(Level.Invocation)
        public void setUp() {
            CONSUMER = KafkaConsumerFactory.create(Collections.singletonList(TOPIC));
            PRODUCER = KafkaProducerFactory.create();
            ACTOR_SYSTEM = ActorSystem.create();
            MATERIALIZER = ActorMaterializer.create(ACTOR_SYSTEM);
            CONSUMER_SETTINGS =
                    ConsumerSettings.create(ACTOR_SYSTEM, new StringDeserializer(), new StringDeserializer())
                            .withBootstrapServers("192.168.0.108:9092")
                            .withGroupId("group-" + RANDOM.nextInt(25))
                            .withProperty("session.timeout.ms", "30000")
                            .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            PRODUCER_SETTINGS = ProducerSettings
                    .create(ACTOR_SYSTEM, new StringSerializer(), new StringSerializer())
                    .withBootstrapServers("192.168.0.108:9092");

            semaphore = new Semaphore(0);
            whenComplete = () -> semaphore.release();

            kafkaPlainConsumer = new KafkaPlainConsumer(CONSUMER);
            reactiveKafkaPlainConsumer = new ReactiveKafkaPlainConsumer(MATERIALIZER, CONSUMER_SETTINGS);
            kafkaAtLeastOnceDeliveryConsumer = new KafkaAtLeastOnceDeliveryConsumer(CONSUMER);
            reactiveKafkaAtLeastOnceDeliveryConsumer = new ReactiveKafkaAtLeastOnceDeliveryConsumer(MATERIALIZER, CONSUMER_SETTINGS);
            kProducer = new KProducer(PRODUCER);
            reactiveKafkaProducer = new ReactiveKafkaProducer(MATERIALIZER, PRODUCER_SETTINGS);
        }

        @TearDown(Level.Invocation)
        public void tearDown() {
            ACTOR_SYSTEM.terminate();
        }
    }

    @Benchmark
    public void b_kafkaPlainConsumerBench(BenchState state) throws InterruptedException {
        state.kafkaPlainConsumer.consume(BenchState.TOPIC, state.limit, 20, state.whenComplete);
        state.semaphore.acquire();
    }

    @Benchmark
    public void b_reactiveKafkaPlainConsumerBench(BenchState state) throws InterruptedException {
        state.reactiveKafkaPlainConsumer.consume(BenchState.TOPIC, state.limit, 20, state.whenComplete);
        System.out.println("exist");
        state.semaphore.acquire();
    }

    @Benchmark
    public void c_kafkaAtLeastOnceDeliveryConsumerBench(BenchState state) throws InterruptedException {
        state.kafkaAtLeastOnceDeliveryConsumer.consume(BenchState.TOPIC, state.limit, 20, state.whenComplete);
        state.semaphore.acquire();
    }

    @Benchmark
    public void c_reactiveKafkaAtLeastOnceDeliveryConsumerBench(BenchState state) throws InterruptedException {
        state.reactiveKafkaAtLeastOnceDeliveryConsumer.consume(BenchState.TOPIC, state.limit, 20, state.whenComplete);
        state.semaphore.acquire();
    }

    @Benchmark
    public void a_kafkaProducerBench(BenchState state) throws InterruptedException {
        state.kProducer.produce(BenchState.TOPIC, BenchState.DATA, state.limit, state.whenComplete);
        state.semaphore.acquire();
    }

    @Benchmark
    public void a_reactiveKafkaProducerBench(BenchState state) throws InterruptedException {
        state.reactiveKafkaProducer.produce(BenchState.TOPIC, BenchState.DATA, state.limit, state.whenComplete);
        state.semaphore.acquire();
    }



}
