package io.micronaut.configuration.kafka.processor;

import io.micronaut.core.annotation.Internal;

/**
 * Defines the strategy for creating consumers defined inside
 * of class annotated with @KafkaListener annotation
 *
 * @author Vladimir Vrab
 */
@Internal
public enum ConsumerCreationStrategy {

    PER_TOPIC,

    PER_CLASS
}
