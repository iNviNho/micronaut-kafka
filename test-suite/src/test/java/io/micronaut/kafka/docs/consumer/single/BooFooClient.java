package io.micronaut.kafka.docs.consumer.single;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Requires;

@Requires(property = "spec.name", value = "ConsumerCreationStrategyTest")
@KafkaClient()
public interface BooFooClient {

    @Topic("boo")
    void sendBoo(Boo boo);

    @Topic("foo")
    void sendFoo(Foo foo);
}
