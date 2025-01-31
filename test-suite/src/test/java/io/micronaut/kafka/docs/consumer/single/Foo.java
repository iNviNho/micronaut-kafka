package io.micronaut.kafka.docs.consumer.single;

import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public record Foo(
    String name
) {
}
