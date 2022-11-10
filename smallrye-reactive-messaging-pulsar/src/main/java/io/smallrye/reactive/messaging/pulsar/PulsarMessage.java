package io.smallrye.reactive.messaging.pulsar;

import java.util.Map;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;

public interface PulsarMessage<T> extends ContextAwareMessage<T> {

    static <T> PulsarOutgoingMessage<T> from(Message<T> message) {
        return PulsarOutgoingMessage.from(message);
    }

    static <T> PulsarMessage<T> of(T payload) {
        return new PulsarOutgoingMessage<>(payload, null, null);
    }

    static <T> PulsarMessage<T> of(T payload, String key) {
        return new PulsarOutgoingMessage<>(payload, null, null, PulsarOutgoingMessageMetadata.builder()
                .withKey(key)
                .build());
    }

    static <T> PulsarMessage<T> of(T payload, byte[] keyBytes) {
        return new PulsarOutgoingMessage<>(payload, null, null, PulsarOutgoingMessageMetadata.builder()
                .withKeyBytes(keyBytes)
                .build());
    }

    static <T> PulsarMessage<T> of(T payload, PulsarOutgoingMessageMetadata metadata) {
        return new PulsarOutgoingMessage<>(payload, null, null, metadata);
    }

    String getKey();

    byte[] getKeyBytes();

    boolean hasKey();

    byte[] getOrderingKey();

    Map<String, String> getProperties();

    long getEventTime();

    long getSequenceId();

}
