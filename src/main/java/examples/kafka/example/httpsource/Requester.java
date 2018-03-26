package examples.kafka.example.httpsource;

import akka.actor.ActorSystem;
import akka.http.javadsl.Http;
import akka.http.javadsl.model.HttpRequest;
import akka.kafka.ProducerMessage;
import akka.kafka.ProducerSettings;
import akka.kafka.javadsl.Producer;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.scaladsl.Framing;
import akka.util.ByteString;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Requester {
    private static final String MAIN = "http://localhost:9999/";
    public static final String STREAM = MAIN + "stream";
    public static final String WHOLE = MAIN + "whole";
    public static final String SLOW_STREAM = MAIN + "slow-stream";
    public static final String SLOW_WHOLE = MAIN + "slow-whole";

    private final String topic = "http-topic";
    private final ActorSystem system = ActorSystem.create();
    private final Materializer materializer = ActorMaterializer.create(system);
    private final ProducerSettings<String, String> producerSettings = ProducerSettings
            .create(system, new StringSerializer(), new StringSerializer())
            .withBootstrapServers("localhost:9092");
    private final KafkaProducer<String, String> producer = producerSettings
            .createKafkaProducer();


    public void runAsync(String url, boolean slowing, Runnable callback) {
        Http.get(system)
                .singleRequest(HttpRequest.create(url))
                .thenAcceptAsync(httpResponse -> {
                    System.out.println(httpResponse.status());
                    httpResponse
                            .entity()
                            .getDataBytes()
                            .via(Framing.delimiter(ByteString.fromString(" "), 256, false))
                            .map(string -> string.decodeString("cp1251"))
                            .map(string -> slowing ? someSlowOperation(string) : string)
                            .map(word -> new ProducerMessage.Message<String, String, String>(
                                    new ProducerRecord<>(topic, word), word))
                            .via(Producer.flow(producerSettings))
                            .runWith(Sink.ignore(), materializer);
                }).whenCompleteAsync((done, throwable) -> {
                    if (throwable != null) {
                        System.out.println("error " + throwable.getMessage());
                    } else {
                        callback.run();
                    }
                }
        );
    }

    private String someSlowOperation(String string) throws InterruptedException {
        Thread.sleep(100);
        return string;
    }
}
