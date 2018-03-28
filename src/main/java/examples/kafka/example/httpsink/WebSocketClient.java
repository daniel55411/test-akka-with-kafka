package examples.kafka.example.httpsink;

import akka.Done;
import akka.http.javadsl.Http;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.model.ws.Message;
import akka.http.javadsl.model.ws.WebSocketRequest;
import akka.http.javadsl.model.ws.WebSocketUpgradeResponse;
import akka.japi.Pair;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import examples.kafka.example.Environment;

import java.util.concurrent.CompletionStage;

public class WebSocketClient {
    private final Sink<Message, CompletionStage<Done>> receiverSink =
            Sink.foreach(param -> System.out.println(param.asTextMessage().getStrictText()));

    private final Environment environment;
    private final Flow<Message, Message, CompletionStage<WebSocketUpgradeResponse>> webSocketFlow;

    public WebSocketClient(Environment environment) {
        this.environment = environment;
        this.webSocketFlow = Http
                .get(environment.getActorSystem())
                .webSocketClientFlow(WebSocketRequest.create("ws://localhost:10000"));
    }

    public Pair<CompletionStage<Done>, CompletionStage<Done>> sendAndListenAsync(Source<Message, ?> source) {
        Pair<CompletionStage<WebSocketUpgradeResponse>, CompletionStage<Done>> pair = pair(source);

        CompletionStage<WebSocketUpgradeResponse> upgradeCompletion = pair.first();
        CompletionStage<Done> closed = pair.second();

        CompletionStage<Done> connected = upgradeCompletion.thenApply(upgrade ->
        {
            if (upgrade.response().status().equals(StatusCodes.SWITCHING_PROTOCOLS)) {
                return Done.getInstance();
            } else {
                throw new RuntimeException(("Connection failed: " + upgrade.response().status()));
            }
        });

        return Pair.create(connected, closed);
    }

    private Pair<CompletionStage<WebSocketUpgradeResponse>, CompletionStage<Done>> pair(
            Source<Message, ?> source) {
        return source
                .viaMat(webSocketFlow, Keep.right())
                .toMat(receiverSink, Keep.both())
                .run(environment.getMaterializer());

    }
}
