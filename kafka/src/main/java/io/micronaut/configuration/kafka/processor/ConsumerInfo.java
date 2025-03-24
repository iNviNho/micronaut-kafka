/*
 * Copyright 2017-2020 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.configuration.kafka.processor;

import io.micronaut.configuration.kafka.KafkaMessage;
import io.micronaut.configuration.kafka.annotation.ErrorStrategy;
import io.micronaut.configuration.kafka.annotation.ErrorStrategyValue;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.configuration.kafka.seek.KafkaSeekOperations;
import io.micronaut.core.annotation.*;
import io.micronaut.core.reflect.ReflectionUtils;
import io.micronaut.core.type.Argument;
import io.micronaut.core.util.ArrayUtils;
import io.micronaut.core.util.StringUtils;
import io.micronaut.inject.ExecutableMethod;
import io.micronaut.messaging.Acknowledgement;
import io.micronaut.messaging.annotation.SendTo;
import io.micronaut.messaging.exceptions.MessagingSystemException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.clients.consumer.Consumer;

/**
 * Internal consumer info.
 *
 * @author Guillermo Calvo
 * @since 5.2
 */
@Internal
final class ConsumerInfo {
    final String clientId;
    @Nullable final String groupId;
    final boolean shouldRedeliver;
    final OffsetStrategy offsetStrategy;
    final ErrorStrategyValue errorStrategy;
    final ConsumerCreationStrategy consumerCreationStrategy;
    final @Nullable Duration retryDelay;
    final int retryCount;
    final boolean shouldHandleAllExceptions;
    final List<Class<? extends Throwable>> exceptionTypes;
    @Nullable final String producerClientId;
    @Nullable final String producerTransactionalId;
    final boolean isTransactional;
    final Map<String, ExecutableMethod<Object, ?>> methods;
    final boolean autoStartup;
    final boolean isBatch;
    final boolean isBlocking;
    final Duration pollTimeout;
    final Map<String, Argument<?>> consumerArg;
    final boolean trackPartitions;
    final boolean shouldSendOffsetsToTransaction;

    @SuppressWarnings("unchecked")
    ConsumerInfo(
        String clientId,
        String groupId,
        OffsetStrategy offsetStrategy,
        AnnotationValue<KafkaListener> kafkaListener,
        List<ExecutableMethod<?, ?>> methods
    ) {
        this.clientId = clientId;
        this.groupId = groupId;
        this.shouldRedeliver = kafkaListener.isTrue("redelivery");
        this.offsetStrategy = offsetStrategy;
        final Optional<AnnotationValue<ErrorStrategy>> errorStrategyAnnotation = kafkaListener.getAnnotation("errorStrategy", ErrorStrategy.class);
        this.errorStrategy = errorStrategyAnnotation.map(a -> a.getRequiredValue(ErrorStrategyValue.class)).orElse(ErrorStrategyValue.NONE); // NOSONAR
        this.retryDelay = errorStrategyAnnotation.flatMap(a -> a.get("retryDelay", Duration.class)).filter(d -> !d.isZero() && !d.isNegative()).orElse(null);
        this.retryCount = errorStrategyAnnotation.map(a -> a.intValue("retryCount").orElse(ErrorStrategy.DEFAULT_RETRY_COUNT)).orElse(0);
        this.shouldHandleAllExceptions = errorStrategyAnnotation.flatMap(a -> a.booleanValue("handleAllExceptions")).orElse(ErrorStrategy.DEFAULT_HANDLE_ALL_EXCEPTIONS);
        this.exceptionTypes = Arrays.stream((Class<? extends Throwable>[]) errorStrategyAnnotation.map(a -> a.classValues("exceptionTypes")).orElse(ReflectionUtils.EMPTY_CLASS_ARRAY)).toList();
        this.producerClientId = kafkaListener.stringValue("producerClientId").orElse(null);
        this.producerTransactionalId = kafkaListener.stringValue("producerTransactionalId").filter(StringUtils::isNotEmpty).orElse(null);
        this.isTransactional = producerTransactionalId != null;
        var firstMethod = methods.stream().findFirst().orElseThrow(() -> new MessagingSystemException("No methods found. Every KafkaListener must provide at least one method"));
        this.autoStartup = kafkaListener.booleanValue("autoStartup").orElse(true);
        this.consumerCreationStrategy = kafkaListener.enumValue("consumerCreationStrategy", ConsumerCreationStrategy.class).orElse(ConsumerCreationStrategy.PER_TOPIC);
        this.isBatch = firstMethod.isTrue(KafkaListener.class, "batch");
        this.isBlocking = firstMethod.hasAnnotation(Blocking.class);
        this.pollTimeout = firstMethod.getValue(KafkaListener.class, "pollTimeout", Duration.class).orElseGet(() -> Duration.ofMillis(100));
        this.consumerArg = resolveConsumerArg(methods);
        this.shouldSendOffsetsToTransaction = offsetStrategy == OffsetStrategy.SEND_TO_TRANSACTION;
        this.methods = resolveMethods(methods);
        this.trackPartitions = anyMethodHasAckArg() || offsetStrategy == OffsetStrategy.SYNC_PER_RECORD || offsetStrategy == OffsetStrategy.ASYNC_PER_RECORD;
        if (shouldSendOffsetsToTransaction) {
            if (!isTransactional || !allMethodsHaveAnnotation(SendTo.class)) {
                throw new MessagingSystemException("Offset strategy 'SEND_TO_TRANSACTION' can only be used when transaction is enabled and @SendTo is used");
            }
            if (shouldRedeliver) {
                throw new MessagingSystemException("Redelivery not supported for transactions in combination with @SendTo");
            }
        }

        if (consumerCreationStrategy == ConsumerCreationStrategy.PER_CLASS && isBatch) {
            throw new MessagingSystemException("Batch mode is not yet supported with consumer creation strategy 'PER_CLASS'");
        }
    }

    private Map<String, Argument<?>> resolveConsumerArg(
        List<ExecutableMethod<?, ?>> methods
    ) {
        return methods.stream().map(item -> {
            var keys = item.getAnnotation(Topic.class).getValues().get("value");
            if (keys == null) {
                keys = item.getAnnotation(Topic.class).getValues().get("patterns");
                if (keys == null) {
                    throw new MessagingSystemException("Missing @Topic annotation");
                }
            }
            var consumerArg = Arrays.stream(item.getArguments()).filter(arg -> Consumer.class.isAssignableFrom(arg.getType())).findFirst().orElse(null);
            return consumerArg != null ? Arrays.stream((String[]) keys).map(key -> Map.entry(key, consumerArg)).collect(Collectors.toList()) : null;
        })
            .filter(Objects::nonNull)
            .flatMap(List::stream)
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (existing, replacement) -> existing,
                HashMap::new
            ));
    }

    private Map<String, ExecutableMethod<Object, ?>> resolveMethods(
        List<ExecutableMethod<?, ?>> methods
    ) {
        try {
            return methods.stream().map(item -> {
                var keys = item.getAnnotation(Topic.class).getValues().get("value");
                if (keys == null) {
                    keys = item.getAnnotation(Topic.class).getValues().get("patterns");
                    if (keys == null) {
                        throw new MessagingSystemException("Missing @Topic annotation");
                    }
                }
                return Arrays.stream((String[]) keys).map(key -> Map.entry(key, item)).collect(Collectors.toList());
            }).flatMap(List::stream).collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> (ExecutableMethod<Object, ?>) entry.getValue()
            ));
        } catch (IllegalStateException e) {
            if (e.getMessage().contains("Duplicate key")) {
                var className = methods.stream().findFirst().get().getTargetMethod().getDeclaringClass();
                throw new MessagingSystemException(
                    "Duplicate topic found in @Topic annotation for " + className + " . Only one topic per listener is allowed.");
            }
            throw e;
        }
    }

    private boolean allMethodsHaveAnnotation(Class<SendTo> sendToClass) {
        return methods.values().stream().allMatch(m -> m.hasAnnotation(sendToClass));
    }

    public String logMethod(String topic) {
        var method = methods.get(topic);
        return method.getDeclaringType().getSimpleName() + "#" + method.getName();
    }

    public List<String> sendToTopics(String consumedRecordTopic) {
        var method = methods.get(consumedRecordTopic);
        return Optional.ofNullable(method.stringValues(SendTo.class)).filter(ArrayUtils::isNotEmpty).stream().flatMap(Arrays::stream).toList();
    }

    public boolean returnsOneKafkaMessage(String topic) {
        var method = methods.get(topic);
        return method.getReturnType().getType().isAssignableFrom(KafkaMessage.class) || method.getReturnType().isAsyncOrReactive() && method.getReturnType().getFirstTypeVariable()
            .map(t -> t.getType().isAssignableFrom(KafkaMessage.class)).orElse(false);
    }

    public boolean returnsManyKafkaMessages(String topic) {
        var method = methods.get(topic);
        return Iterable.class.isAssignableFrom(method.getReturnType().getType()) && method.getReturnType().getFirstTypeVariable()
            .map(t -> t.getType().isAssignableFrom(KafkaMessage.class)).orElse(false);
    }

    public Argument<?> seekArg(String topic) {
        var method = methods.get(topic);
        return Arrays.stream(method.getArguments()).filter(arg -> KafkaSeekOperations.class.isAssignableFrom(arg.getType())).findFirst().orElse(null);
    }

    public Argument<?> consumerArg(String topic) {
        return consumerArg.get(topic);
    }

    public Argument<?> ackArg(String topic) {
        var method = methods.get(topic);
        return Arrays.stream(method.getArguments()).filter(arg -> Acknowledgement.class.isAssignableFrom(arg.getType())).findFirst().orElse(null);
    }

    private boolean anyMethodHasAckArg() {
        return methods.values().stream().anyMatch(m -> Arrays.stream(m.getArguments()).anyMatch(arg -> Acknowledgement.class.isAssignableFrom(arg.getType())));
    }
}
