package examples.kafka.example.httpsource;

import akka.Done;
import akka.http.javadsl.Http;
import akka.http.javadsl.model.HttpRequest;
import akka.kafka.ProducerMessage;
import akka.kafka.javadsl.Producer;
import akka.stream.javadsl.Sink;
import akka.stream.scaladsl.Framing;
import akka.util.ByteString;
import examples.kafka.example.Environment;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.CompletionStage;

public class KafkaSink {
    private static final String MAIN = "http://localhost:9999/";
    public static final String STREAM = MAIN + "stream";
    public static final String WHOLE = MAIN + "whole";
    public static final String SLOW_STREAM = MAIN + "slow-stream";
    public static final String SLOW_WHOLE = MAIN + "slow-whole";

    private final String topic = "http-topic";
    private final Environment environment;

    public KafkaSink(Environment environment) {
        this.environment = environment;
    }

    public CompletionStage<Done> runAsync(String url, boolean slowing) {
        return Http.get(environment.getActorSystem())
                .singleRequest(HttpRequest.create(url))
                .thenApplyAsync(httpResponse -> {
                    System.out.println(httpResponse.status());
                    return httpResponse
                            .entity()
                            .getDataBytes()
                            .via(Framing.delimiter(ByteString.fromString(" "), 256, false))
                            .map(string -> string.decodeString("cp1251"))
                            .map(string -> slowing ? someSlowOperation(string) : string)
                            .map(word -> new ProducerMessage.Message<String, String, String>(
                                    new ProducerRecord<>(topic, word), word))
                            .via(Producer.flow(environment.getProducerSettings()))
                            .runWith(Sink.ignore(), environment.getMaterializer())
                            .toCompletableFuture()
                            .join();
                });
//                .whenCompleteAsync((done, throwable) -> {
//                    if (throwable != null) {
//                        System.out.println("error " + throwable.getMessage());
//                    } else {
//                        callback.run();
//                    }
//                }
    }

    private String someSlowOperation(String string) throws InterruptedException {
        Thread.sleep(100);
        return string;
    }
}
