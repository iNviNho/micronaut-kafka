package io.micronaut.kafka.docs.consumer.single;

import static org.slf4j.LoggerFactory.getLogger;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.configuration.kafka.processor.ConsumerCreationStrategy;
import io.micronaut.context.annotation.Requires;
import org.slf4j.Logger;

@Requires(property = "spec.name", value = "ConsumerCreationStrategyTest")
@KafkaListener(
    value = "myGroup",
    consumerCreationStrategy = ConsumerCreationStrategy.PER_TOPIC,
    offsetReset = OffsetReset.EARLIEST
)
public class MultiTopicListener {
    private static final Logger LOG = getLogger(MultiTopicListener.class);

    int countBoo = 0;
    int countFoo = 0;

    @Topic({"boo"})
    void processBoo(Boo value) {
        LOG.info("Handling boo: {}", value);
        this.countBoo++;
    }

    @Topic({"foo"})
    void processFoo(Foo value) {
        LOG.info("Handling foo: {}", value);
        this.countFoo++;
    }
}
