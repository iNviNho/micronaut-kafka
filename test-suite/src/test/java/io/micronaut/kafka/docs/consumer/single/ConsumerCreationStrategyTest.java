package io.micronaut.kafka.docs.consumer.single;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

import io.micronaut.context.ApplicationContext;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class ConsumerCreationStrategyTest {

    @Test
    void itCreatesOneConsumerForMultipleMethodsAnnotatedWithTopicAnnotation() {
        try (ApplicationContext ctx = ApplicationContext.run(
            Map.of("kafka.enabled", "true", "spec.name", "ConsumerCreationStrategyTest")
        )) {
            Boo boo = new Boo("Boo");
            Foo foo = new Foo("Foo");
            BooFooClient client = ctx.getBean(BooFooClient.class);

            client.sendBoo(boo);
            client.sendBoo(boo);
            client.sendFoo(foo);
            client.sendFoo(foo);

            MultiTopicListener listener = ctx.getBean(MultiTopicListener.class);
            await().atMost(10, SECONDS).until(() ->
                listener.count == 4
            );
        }
    }

}
