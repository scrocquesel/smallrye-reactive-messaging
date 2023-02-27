package io.smallrye.reactive.messaging.rabbitmq.fault;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import io.smallrye.common.annotation.Identifier;
import io.vertx.mutiny.rabbitmq.RabbitMQMessage;
import io.vertx.rabbitmq.RabbitMQClient;
import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigValue;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.eclipse.microprofile.config.spi.Converter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.EmitterConfiguration;
import io.smallrye.reactive.messaging.annotations.EmitterFactoryFor;
import io.smallrye.reactive.messaging.providers.extension.MutinyEmitterImpl;
import io.smallrye.reactive.messaging.rabbitmq.ConnectionHolder;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMessage;
import io.smallrye.reactive.messaging.rabbitmq.OutgoingRabbitMQMetadata;
import io.smallrye.reactive.messaging.rabbitmq.OutgoingRabbitMQMetadata.Builder;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQConnector;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQConnectorIncomingConfiguration;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Context;

public class RabbitMQReconsumeLater implements RabbitMQFailureHandler {
    @ApplicationScoped
    @Identifier("reconsume-later")
    public static class Factory implements RabbitMQFailureHandler.Factory {
        @Override
        public RabbitMQFailureHandler create(RabbitMQConnectorIncomingConfiguration config,
                RabbitMQConnector connector) {
            return new RabbitMQReconsumeLater(connector, config);
        }
    }

    private MutinyEmitterImpl<Object> emitter;

    private String channelName;

    public RabbitMQReconsumeLater(RabbitMQConnector connector, RabbitMQConnectorIncomingConfiguration ic) {

        // create outgoing config for a delayed channel from the incoming channel
        // configuration
        channelName = ic.getChannel() + "-reconsume";
        Map<String, Object> map = new HashMap<>();
        map.put(ConnectorFactory.CHANNEL_NAME_ATTRIBUTE, channelName);
        map.put("exchange.name", "");

        Config config = merge(ic.config(), map);
        emitter = new MutinyEmitterImpl<>(new EmitterConfiguration() {
            @Override
            public String name() {
                return channelName;
            }

            @Override
            public EmitterFactoryFor emitterType() {
                return EmitterFactoryFor.Literal.MUTINY_EMITTER;
            }

            @Override
            public OnOverflow.Strategy overflowBufferStrategy() {
                return OnOverflow.Strategy.BUFFER;
            }

            @Override
            public long overflowBufferSize() {
                return 10;
            }

            @Override
            public boolean broadcast() {
                return true;
            }

            @Override
            public int numberOfSubscriberBeforeConnecting() {
                return 1;
            }
        }, 10);

        Multi.createFrom().publisher(emitter.getPublisher())
                .subscribe((Flow.Subscriber<? super Message<?>>) connector.getSubscriber(config));
        

        // establish queue
        final var client = connector.getClient(channelName);
        client.getDelegate().addConnectionEstablishedCallback(promise -> {
            final JsonObject queueArgs = new JsonObject();
            // deliver the message back to the original queue when the message expire
            queueArgs.put("x-dead-letter-exchange", "");
            queueArgs.put("x-dead-letter-routing-key", ic.getQueueName());
            client.queueDeclare(channelName, true, false, false, queueArgs)
                    // Ensure we create the queue to which messages are to be sent
                    .subscribe().with((ignored) -> promise.complete(), promise::fail);
        });
    }

    @Override
    public <V> CompletionStage<Void> handle(IncomingRabbitMQMessage<V> msg, Context context, Throwable reason) {

        int attemptLeft = (int) msg.getHeaders().getOrDefault("x-attempt-left", 1);
        if (attemptLeft == 0) {
            // todo use a failure strategy ?
            return ConnectionHolder.runOnContext(context, msg, m -> m.rejectMessage(reason));
        } else {
            Builder metadataBuilder = OutgoingRabbitMQMetadata.builder()
                    .withExpiration("1000")
                    .withRoutingKey(channelName);
                    metadataBuilder.withHeader("x-attempt-left", --attemptLeft);

            Message<?> message = Message.of(msg.getPayload(), Metadata.of(metadataBuilder
                    .build()), () -> {
                        System.out.println("ack ");
                        // todo ack/auto-ack
                        return ConnectionHolder.runOnContext(context, msg, IncomingRabbitMQMessage::acknowledgeMessage);
                    }, e -> {
                        System.out.println("nack ");
                        return ConnectionHolder.runOnContext(context, msg, m -> m.rejectMessage(reason));
                    });
            return emitter.sendMessage(message)
                    .runSubscriptionOn(context::runOnContext)
                    .subscribeAsCompletionStage();
        }
    }

    public static Config merge(Config passedCfg, Map<String, Object> mapCfg) {

        if (mapCfg.isEmpty()) {
            return passedCfg;
        }

        return new Config() {

            private <T> T extractValue(String name, Class<T> clazz, boolean failIfMissing) {
                Object value = mapCfg.get(name);

                if (value == null) {
                    value = passedCfg.getOptionalValue(name, clazz).orElse(null);
                    if (value != null) {
                        // noinspection unchecked
                        return (T) value;
                    }
                }

                if (value == null) {
                    if (failIfMissing) {
                        throw new IllegalArgumentException(name);
                    }
                    return null;
                }

                if (clazz.isInstance(value)) {
                    // noinspection unchecked
                    return (T) value;
                }

                // Attempt a conversion
                if (value instanceof String) {
                    String v = (String) value;
                    Optional<Converter<T>> converter = passedCfg.getConverter(clazz);
                    if (converter.isPresent()) {
                        return converter.get().convert(v);
                    }
                    if (failIfMissing) {
                        throw new IllegalArgumentException(name);
                    }
                    return null;
                }

                if (failIfMissing) {
                    throw new IllegalArgumentException(name);
                } else {
                    return null;
                }
            }

            @Override
            public <T> T getValue(String propertyName, Class<T> propertyType) {
                return extractValue(propertyName, propertyType, true);
            }

            @Override
            public ConfigValue getConfigValue(String propertyName) {
                // We only compute ConfigValue for the original config.
                return passedCfg.getConfigValue(propertyName);
            }

            @Override
            public <T> Optional<T> getOptionalValue(String propertyName, Class<T> propertyType) {
                T value = extractValue(propertyName, propertyType, false);
                return Optional.ofNullable(value);
            }

            @Override
            public Iterable<String> getPropertyNames() {
                Set<String> result = new HashSet<>();

                // First global
                result.addAll(mapCfg.keySet());

                // Channel
                Iterable<String> names = passedCfg.getPropertyNames();
                names.forEach(result::add);

                return result;
            }

            @Override
            public Iterable<ConfigSource> getConfigSources() {
                return passedCfg.getConfigSources();
            }

            @Override
            public <T> Optional<Converter<T>> getConverter(Class<T> forType) {
                return passedCfg.getConverter(forType);
            }

            @Override
            public <T> T unwrap(Class<T> type) {
                return passedCfg.unwrap(type);
            }
        };
    }

}
