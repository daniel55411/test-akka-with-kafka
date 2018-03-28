package examples.kafka.example.flow;

import akka.Done;
import akka.kafka.ConsumerMessage;
import akka.kafka.ProducerMessage;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import examples.kafka.example.Environment;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public class KafkaFlow {
    private final Environment environment;

    public KafkaFlow(Environment environment) {
        this.environment = environment;
    }

    public CompletionStage<Done> runAsync(Set<String> src, String dst, Function<String, String> handler) {
        return Consumer.committableSource(environment.getConsumerSettings(), Subscriptions.topics(src))
                .map(param -> new ProducerMessage.Message<String, String, ConsumerMessage.Committable>(
                        new ProducerRecord<String, String>(dst, handler.apply(param.record().value())), param.committableOffset()
                ))
                .runWith(Producer.commitableSink(environment.getProducerSettings()), environment.getMaterializer());
    }
}
