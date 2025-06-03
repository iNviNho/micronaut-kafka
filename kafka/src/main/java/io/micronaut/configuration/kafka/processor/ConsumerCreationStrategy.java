package io.micronaut.configuration.kafka.processor;

/**
 * Defines the strategy for creating consumers defined inside
 * of class annotated with @KafkaListener annotation
 *
 * @author Vladimir Vrab
 * @since 5.9.0
 */
public enum ConsumerCreationStrategy {

    /**
     * Creates a separate Kafka consumer for each method
     * that listens to a different topic.
     *
     * <p>This allows parallel consumption across topics, but results in more
     * consumer instances.</p>
     */
    PER_TOPIC,

    /**
     * Creates a single Kafka consumer for the entire class,
     * regardless of how many methods or topics it listens to.
     *
     * <p>This reduces the number of consumer instances, but may serialize
     * consumption across topics.</p>
     */
    PER_CLASS
}
