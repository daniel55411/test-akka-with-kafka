package examples.kafka.example.httpsink;

import akka.Done;
import akka.http.javadsl.model.ws.Message;
import akka.http.javadsl.model.ws.TextMessage;
import akka.japi.Pair;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.javadsl.Source;
import examples.kafka.example.Environment;

import java.util.Set;
import java.util.concurrent.CompletionStage;

public class KafkaSource {
    private final Environment environment;
    private final WebSocketClient webSocketClient;

    public KafkaSource(Environment environment) {
        this.environment = environment;
        this.webSocketClient = new WebSocketClient(environment);
    }

    public CompletionStage<Done> runAsync(Set<String> topics) {
        Source<Message, Consumer.Control> source = Consumer
                .committableSource(environment.getConsumerSettings(), Subscriptions.topics(topics))
                .map(param -> TextMessage.create(param.record().value()));

        Pair<CompletionStage<Done>, CompletionStage<Done>> pair = webSocketClient.sendAndListenAsync(source);
        CompletionStage<Done> connected = pair.first();
        CompletionStage<Done> closed = pair.second();

        closed.thenAccept(done -> System.out.println("Connection closed"));

        return connected;
    }
}
