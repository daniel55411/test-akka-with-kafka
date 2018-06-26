package examples.kafka.example;

import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.ProducerSettings;
import akka.stream.Materializer;

public interface Environment<TKey, TValue> {
    ActorSystem getActorSystem();
    Materializer getMaterializer();
    ProducerSettings<TKey, TValue> getProducerSettings();
    ConsumerSettings<TKey, TValue> getConsumerSettings();
}
