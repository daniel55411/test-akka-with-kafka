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

public class Benchmarks {
    @State(Scope.Benchmark)
    public static class BenchState {
        static final String TOPIC = "bench-1";
        static final List<String> DATA = Arrays.asList("data-1", "data-2", "data-3", "data-4");

        private static final Consumer<String, String> CONSUMER = KafkaConsumerFactory.create(Collections.singletonList(TOPIC));
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

        KafkaPlainConsumer kafkaPlainConsumer;
        ReactiveKafkaPlainConsumer reactiveKafkaPlainConsumer;
        KafkaAtLeastOnceDeliveryConsumer kafkaAtLeastOnceDeliveryConsumer;
        ReactiveKafkaAtLeastOnceDeliveryConsumer reactiveKafkaAtLeastOnceDeliveryConsumer;
        KProducer kProducer;
        ReactiveKafkaProducer reactiveKafkaProducer;

        @Param({ "1000", "100000" })
        public int limit;


        @Setup(Level.Trial)
        public void setUp() {
            kafkaPlainConsumer = new KafkaPlainConsumer(CONSUMER);
            reactiveKafkaPlainConsumer = new ReactiveKafkaPlainConsumer(MATERIALIZER, CONSUMER_SETTINGS);
            kafkaAtLeastOnceDeliveryConsumer = new KafkaAtLeastOnceDeliveryConsumer(CONSUMER);
            reactiveKafkaAtLeastOnceDeliveryConsumer = new ReactiveKafkaAtLeastOnceDeliveryConsumer(MATERIALIZER, CONSUMER_SETTINGS);
            kProducer = new KProducer(PRODUCER);
            reactiveKafkaProducer = new ReactiveKafkaProducer(MATERIALIZER, PRODUCER_SETTINGS);
        }

        @TearDown
        public void tearDown() {
            ACTOR_SYSTEM.terminate();
        }
    }

//    @Benchmark
    public void b_kafkaPlainConsumerBench(BenchState state) {
        state.kafkaPlainConsumer.consume(BenchState.TOPIC, state.limit, 20);
    }

    @Benchmark
    public void b_reactiveKafkaPlainConsumerBench(BenchState state) {
        state.reactiveKafkaPlainConsumer.consume(BenchState.TOPIC, state.limit, 20);
    }

//    @Benchmark
    public void c_kafkaAtLeastOnceDeliveryConsumerBench(BenchState state) {
        state.kafkaAtLeastOnceDeliveryConsumer.consume(BenchState.TOPIC, state.limit, 20);
    }

    @Benchmark
    public void c_reactiveKafkaAtLeastOnceDeliveryConsumerBench(BenchState state) {
        state.reactiveKafkaAtLeastOnceDeliveryConsumer.consume(BenchState.TOPIC, state.limit, 20);
    }

//    @Benchmark
    public void a_kafkaProducerBench(BenchState state) {
        state.kProducer.produce(BenchState.TOPIC, BenchState.DATA, state.limit);
    }

    @Benchmark
    public void a_reactiveKafkaProducerBench(BenchState state) {
        state.reactiveKafkaProducer.produce(BenchState.TOPIC, BenchState.DATA, state.limit);
    }



}
