package examples.kafka.example.httpsink;

import akka.NotUsed;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.ws.Message;
import akka.http.javadsl.model.ws.TextMessage;
import akka.http.javadsl.model.ws.WebSocket;
import akka.stream.javadsl.Flow;
import examples.kafka.example.Environment;
import examples.kafka.example.LocalEnvironment;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;

public class WebSocketServer {
    public final static Consumer<Message> DEFAULT_MESSAGE_CONSUMER = message -> {
        TextMessage textMessage = message.asTextMessage();
        if (textMessage.isStrict()) {
            System.out.println(textMessage.getStrictText());
        } else {
            System.out.println(textMessage.getStreamedText().reduce(String::concat).toString());
        }
    };

    private final static Message ACK = TextMessage.create("ACK");

    private final Consumer<Message> messageConsumer;
    private final Function<HttpRequest, HttpResponse> handler =
            httpRequest -> WebSocket.handleWebSocketRequestWith(httpRequest, acknowledgment());
    private final Environment environment;

    public static WebSocketServer newAndStart(Environment environment, Consumer<Message> consumer)
            throws InterruptedException, ExecutionException, TimeoutException {
        return new WebSocketServer(environment, consumer);
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException, TimeoutException {
        Environment environment = new LocalEnvironment();
        WebSocketServer webSocketServer = WebSocketServer.newAndStart(
                environment,
                WebSocketServer.DEFAULT_MESSAGE_CONSUMER
        );
    }

    private WebSocketServer(Environment environment, Consumer<Message> consumer)
            throws InterruptedException, ExecutionException, TimeoutException {
        this.messageConsumer = consumer;
        this.environment = environment;

        CompletionStage<ServerBinding> serverBindingCompletionStage = Http.get(environment.getActorSystem())
                .bindAndHandleSync(
                        handler::apply,
                        ConnectHttp.toHost("localhost", 10000),
                        environment.getMaterializer()
                );

        serverBindingCompletionStage
                .toCompletableFuture()
                .get(1, TimeUnit.SECONDS);
    }

    private Flow<Message, Message, NotUsed> acknowledgment() {
        return Flow
                .<Message>create()
                .map(param -> {
                    messageConsumer.accept(param);
                    return ACK;
                });
    }
}
