package io.smallrye.reactive.messaging.pulsar.fault;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.pulsar.PulsarConnector;
import io.smallrye.reactive.messaging.pulsar.base.WeldTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class PulsarNackTest extends WeldTestBase {

    public static final int NUMBER_OF_MESSAGES = 100;

    @Test
    void testFailStop() throws PulsarClientException {
        addBeans(PulsarFailStop.Factory.class);
        // Run app
        FailingConsumingApp app = runApplication(config()
                .with("mp.messaging.incoming.data.failure-strategy", "fail-stop"), FailingConsumingApp.class);
        // Produce messages
        send(client.newProducer(Schema.INT32)
                .producerName("test-producer")
                .topic(topic)
                .create(), NUMBER_OF_MESSAGES, i -> i);

        // Check for consumed messages in app
        await().untilAsserted(() -> {
            assertThat(app.getResults()).hasSize(0);
            assertThat(app.getFailures()).hasSize(1);
        });
    }

    @Test
    void testIgnore() throws PulsarClientException {
        addBeans(PulsarIgnore.Factory.class);
        // Run app
        FailingConsumingApp app = runApplication(config()
                .with("mp.messaging.incoming.data.failure-strategy", "ignore"), FailingConsumingApp.class);
        // Produce messages
        send(client.newProducer(Schema.INT32)
                .producerName("test-producer")
                .topic(topic)
                .create(), NUMBER_OF_MESSAGES, i -> i);

        // Check for consumed messages in app
        await().untilAsserted(() -> {
            assertThat(app.getResults()).hasSize(NUMBER_OF_MESSAGES - 10);
            assertThat(app.getFailures()).hasSize(10);
        });
    }

    @Test
    void testNack() throws PulsarClientException {
        addBeans(PulsarNack.Factory.class);
        // Run app
        FailingConsumingApp app = runApplication(config(), FailingConsumingApp.class);
        // Produce messages
        send(client.newProducer(Schema.INT32)
                .producerName("test-producer")
                .topic(topic)
                .create(), NUMBER_OF_MESSAGES, i -> i);

        // Check for consumed messages in app
        await().untilAsserted(() -> {
            assertThat(app.getResults()).hasSize(NUMBER_OF_MESSAGES - 10);
            assertThat(app.getFailures()).hasSize(10);
        });
    }

    @Test
    void testReconsumeLater() throws PulsarClientException {
        addBeans(PulsarReconsumeLater.Factory.class);
        // Run app
        FailingConsumingApp app = runApplication(config()
                .with("mp.messaging.incoming.data.failure-strategy", "reconsume-later")
                .with("mp.messaging.incoming.data.deadLetterPolicy.retryLetterTopic", topic + "-retry")
                .with("mp.messaging.incoming.data.deadLetterPolicy.maxRedeliverCount", 2)
                .with("mp.messaging.incoming.data.retryEnable", true)
                .with("mp.messaging.incoming.data.subscriptionType", "Shared"), FailingConsumingApp.class);
        // Produce messages
        send(client.newProducer(Schema.INT32)
                .producerName("test-producer")
                .topic(topic)
                .create(), NUMBER_OF_MESSAGES, i -> i);

        // Check for consumed messages in app
        await().untilAsserted(() -> {
            assertThat(app.getResults()).hasSize(NUMBER_OF_MESSAGES - 10);
            assertThat(app.getFailures()).hasSize(30);
        });

        List<Message<Integer>> retries = new CopyOnWriteArrayList<>();
        receive(client.newConsumer(Schema.INT32)
                .topic(topic + "-retry")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("subscription")
                .subscribe(), 20, retries::add);

        // Check for retried messages
        await().untilAsserted(() -> assertThat(retries).hasSize(20));
    }

    @ApplicationScoped
    public static class FailingConsumingApp {

        private final List<Integer> results = new CopyOnWriteArrayList<>();
        private final List<Integer> failures = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public void consume(Integer message) {
            if (message % 10 == 0) {
                failures.add(message);
                throw new IllegalArgumentException("boom");
            }
            results.add(message);
        }

        public List<Integer> getResults() {
            return results;
        }

        public List<Integer> getFailures() {
            return failures;
        }
    }

    MapBasedConfig config() {
        // TODO complete config
        return new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.serviceUrl", serviceUrl)
                .with("mp.messaging.incoming.data.topic", topic)
                .with("mp.messaging.incoming.data.schema", "INT32");
    }

}
