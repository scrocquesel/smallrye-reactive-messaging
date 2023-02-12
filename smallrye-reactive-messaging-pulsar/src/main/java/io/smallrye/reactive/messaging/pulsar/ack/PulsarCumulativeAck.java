package io.smallrye.reactive.messaging.pulsar.ack;

import javax.enterprise.context.ApplicationScoped;

import org.apache.pulsar.client.api.Consumer;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.pulsar.PulsarAckHandler;
import io.smallrye.reactive.messaging.pulsar.PulsarConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.pulsar.PulsarIncomingMessage;

public class PulsarCumulativeAck implements PulsarAckHandler {

    public static final String STRATEGY_NAME = "cumulative";

    @ApplicationScoped
    @Identifier(STRATEGY_NAME)
    public static class Factory implements PulsarAckHandler.Factory {

        @Override
        public PulsarCumulativeAck create(Consumer<?> consumer, PulsarConnectorIncomingConfiguration config) {
            return new PulsarCumulativeAck(consumer);
        }
    }

    private final Consumer<?> consumer;

    public PulsarCumulativeAck(Consumer<?> consumer) {
        this.consumer = consumer;
    }

    @Override
    public Uni<Void> handle(PulsarIncomingMessage<?> message) {
        return Uni.createFrom().completionStage(() -> consumer.acknowledgeCumulativeAsync(message.getMessageId()))
                .emitOn(message::runOnMessageContext);
    }
}
