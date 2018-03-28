package examples.kafka.example;

import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.ProducerSettings;
import akka.stream.Materializer;

public interface Environment {
    ActorSystem getActorSystem();
    Materializer getMaterializer();
    ProducerSettings<String, String> getProducerSettings();
    ConsumerSettings<String, String> getConsumerSettings();
}
