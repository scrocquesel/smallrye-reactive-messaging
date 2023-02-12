package io.smallrye.reactive.messaging.pulsar.fault;

import javax.enterprise.context.ApplicationScoped;

import org.apache.pulsar.client.api.Consumer;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.pulsar.PulsarConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.pulsar.PulsarFailureHandler;
import io.smallrye.reactive.messaging.pulsar.PulsarIncomingMessage;
import io.smallrye.reactive.messaging.pulsar.i18n.PulsarLogging;

public class PulsarFailStop implements PulsarFailureHandler {
    public static final String STRATEGY_NAME = "fail-stop";

    @ApplicationScoped
    @Identifier(STRATEGY_NAME)
    public static class Factory implements PulsarFailureHandler.Factory {

        @Override
        public PulsarFailStop create(Consumer<?> consumer, PulsarConnectorIncomingConfiguration config) {
            return new PulsarFailStop(consumer, config.getChannel());
        }
    }

    private final Consumer<?> consumer;
    private final String channel;

    public PulsarFailStop(Consumer<?> consumer, String channel) {
        this.consumer = consumer;
        this.channel = channel;
    }

    @Override
    public Uni<Void> handle(PulsarIncomingMessage<?> message, Throwable reason, Metadata metadata) {
        // We don't commit, we just fail and stop the client.
        PulsarLogging.log.messageNackedFailStop(channel);
        // TODO report failure to the connector for health check
        return Uni.createFrom().<Void> failure(reason)
                .emitOn(message::runOnMessageContext);
    }
}
