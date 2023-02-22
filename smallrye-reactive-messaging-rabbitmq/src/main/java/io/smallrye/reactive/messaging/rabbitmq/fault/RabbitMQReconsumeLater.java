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
import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigValue;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.eclipse.microprofile.config.spi.Converter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.EmitterConfiguration;
import io.smallrye.reactive.messaging.annotations.EmitterFactoryFor;
import io.smallrye.reactive.messaging.providers.extension.MutinyEmitterImpl;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMessage;
import io.smallrye.reactive.messaging.rabbitmq.OutgoingRabbitMQMetadata;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQConnector;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQConnectorIncomingConfiguration;
import io.vertx.mutiny.core.Context;

public class RabbitMQReconsumeLater implements RabbitMQFailureHandler {
    @ApplicationScoped
    @Identifier("reconsume-later")
    public static class Factory implements RabbitMQFailureHandler.Factory {
        @Override
        public RabbitMQFailureHandler create(RabbitMQConnectorIncomingConfiguration config, RabbitMQConnector connector) {
            return new RabbitMQReconsumeLater(connector, config);
        }
    }

    private MutinyEmitterImpl<Object> emitter;

    public RabbitMQReconsumeLater(RabbitMQConnector connector, RabbitMQConnectorIncomingConfiguration ic) {

        // create outgoing config for a delayed channel from the incoming channel configuration
        Map<String, Object> map = new HashMap<>();
        map.put(ConnectorFactory.CHANNEL_NAME_ATTRIBUTE, ic.getChannel() + "-reconsume");
        map.put("exchange.declare", "false");
        map.put("queue.declare", "false");
        map.put("default-ttl", "10000");
        Config config = merge(ic.config(), map);
        emitter = new MutinyEmitterImpl<>(new EmitterConfiguration() {
            @Override
            public String name() {
                return ic.getChannel() + "-reconsume";
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
                .onItem()
                .transformToUniAndConcatenate(m -> Uni.createFrom().item(m).onItem().delayIt().by(Duration.ofSeconds(1)))
                .subscribe((Flow.Subscriber<? super Message<?>>) connector.getSubscriber(config));
    }

    @Override
    public <V> CompletionStage<Void> handle(IncomingRabbitMQMessage<V> msg, Context context, Throwable reason) {
        Message<?> message = msg.addMetadata(OutgoingRabbitMQMetadata.builder()
                // TODO new outgoing message with ttl from msg
                .build());
        return emitter.sendMessage(message)
                .runSubscriptionOn(context::runOnContext)
                .invoke(x -> {
                    RabbitMQMessage rabbitMQMessage = msg.getRabbitMQMessage();
                    System.out.println("sent " + rabbitMQMessage.envelope().getExchange() + " " + rabbitMQMessage.body().toString());
                })
                .subscribeAsCompletionStage();
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
                        //noinspection unchecked
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
                    //noinspection unchecked
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
