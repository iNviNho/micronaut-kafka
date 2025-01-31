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
    consumerCreationStrategy = ConsumerCreationStrategy.PER_CLASS,
    offsetReset = OffsetReset.EARLIEST
)
public class MultiTopicListener {
    private static final Logger LOG = getLogger(MultiTopicListener.class);

    int count = 0;

    @Topic({"boo", "too"})
    void processBoo(String value) {
        LOG.info("Handling boo: {}", value);
        this.count++;
    }

    @Topic("foo")
    void processFoo(String value) {
        LOG.info("Handling foo: {}", value);
        this.count++;
    }
}
