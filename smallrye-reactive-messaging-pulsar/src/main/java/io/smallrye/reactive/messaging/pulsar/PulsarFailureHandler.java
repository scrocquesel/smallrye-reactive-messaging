package io.smallrye.reactive.messaging.pulsar;

import org.apache.pulsar.client.api.Consumer;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.mutiny.Uni;

public interface PulsarFailureHandler {

    interface Factory {
        PulsarFailureHandler create(Consumer<?> consumer, PulsarConnectorIncomingConfiguration config);
    }

    Uni<Void> handle(PulsarIncomingMessage<?> message, Throwable reason, Metadata metadata);

}
