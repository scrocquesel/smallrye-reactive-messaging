package io.smallrye.reactive.messaging.rabbitmq.fault;

import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQExceptions.ex;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigValue;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.eclipse.microprofile.config.spi.Converter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.EmitterConfiguration;
import io.smallrye.reactive.messaging.annotations.EmitterFactoryFor;
import io.smallrye.reactive.messaging.providers.extension.MutinyEmitterImpl;
import io.smallrye.reactive.messaging.providers.helpers.CDIUtils;
import io.smallrye.reactive.messaging.rabbitmq.ConnectionHolder;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMessage;
import io.smallrye.reactive.messaging.rabbitmq.OutgoingRabbitMQMetadata;
import io.smallrye.reactive.messaging.rabbitmq.OutgoingRabbitMQMetadata.Builder;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQConnector;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.rabbitmq.ack.RabbitMQAckHandler;
import io.vertx.mutiny.core.Context;

public class RabbitMQReconsumeLater implements RabbitMQFailureHandler {

    private static final String identifier = "reconsume-later";

    @ApplicationScoped
    @Identifier(identifier)
    public static class Factory implements RabbitMQFailureHandler.Factory {

        @Inject
        @Any
        Instance<RabbitMQFailureHandler.Factory> failureHandlerFactories;

        @Override
        public RabbitMQFailureHandler create(RabbitMQConnectorIncomingConfiguration config,
                RabbitMQConnector connector) {

            return new RabbitMQReconsumeLater(connector, config, failureHandlerFactories);
        }
    }

    private MutinyEmitterImpl<Object> emitter;
    private String reconsumeQueueName;
    private Optional<Integer> maxDeliveryCount;
    private String deliveryCountName;
    private RabbitMQAckHandler ackHandler;
    private RabbitMQFailureHandler nackHandler;
    private RabbitMQFailureHandler tooManyDeliveryNackHandler;

    public RabbitMQReconsumeLater(RabbitMQConnector connector, RabbitMQConnectorIncomingConfiguration ic,
            Instance<RabbitMQFailureHandler.Factory> failureHandlerFactories) {
        reconsumeQueueName = ic.getReconsumeQueueName()
                .orElse(String.format("%s.reconsume", ic.getQueueName()));
        maxDeliveryCount = ic.getReconsumeQueueDeliveryLimit();
        deliveryCountName = ic.getReconsumeQueueDeliveryLimitName();
        ackHandler = connector.createAckHandler(ic);
        nackHandler = Boolean.TRUE.equals(ic.getAutoAcknowledgement()) ? new RabbitMQFailureHandler() {
            @Override
            public <V> CompletionStage<Void> handle(IncomingRabbitMQMessage<V> msg, Context context,
                    Throwable reason) {
                return CompletableFuture.completedStage(null);
            }
        } : new RabbitMQFailureHandler() {
            @Override
            public <V> CompletionStage<Void> handle(IncomingRabbitMQMessage<V> msg, Context context,
                    Throwable reason) {
                // if an error occur while publishing the reconsume copy, requeue the original
                // message
                return ConnectionHolder.runOnContext(context, msg, m -> m.nack(reason));
            }
        };
        tooManyDeliveryNackHandler = createFailureHandler(ic.getReconsumeQueueDeliveryLimitFailureStrategy(), ic, connector,
                failureHandlerFactories);

        final String channelName = ic.getChannel() + "-reconsume";
        final int overflowBufferSize = ic.getMaxIncomingInternalQueueSize(); // ?

        // create outgoing config for a delayed channel from the incoming channel
        // configuration
        Map<String, Object> map = new HashMap<>();
        map.put(ConnectorFactory.CHANNEL_NAME_ATTRIBUTE, channelName);
        map.put("exchange.name", ""); // use default amqp exchange
        map.put("exchange.declare", false);
        map.put("exchange.auto-delete", false);
        map.put("bindings", null);
        map.put("dlx.declare", false);

        // override queue params for establishing reconsume queue
        map.put("queue.declare", ic.getReconsumeQueueDeclare());
        map.put("queue.durable", ic.getReconsumeQueueDurable());
        map.put("queue.name", reconsumeQueueName);
        map.put("queue.ttl", ic.getReconsumeQueueTtl());
        map.put("queue.x-queue-mode", ic.getReconsumeQueueXQueueMode().orElse(null));
        map.put("queue.x-queue-type", ic.getReconsumeQueueXQueueType().orElse(null));
        map.put("auto-bind-dlq", true);
        map.put("dead-letter-exchange", ""); // use default amqp exchange
        map.put("dead-letter-routing-key", ic.getQueueName());
        map.put("queue.single-active-consumer", null);
        map.put("queue.x-max-priority", null);
        map.put("queue.x-delivery-limit", null);

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
                return overflowBufferSize;
            }

            @Override
            public boolean broadcast() {
                return false;
            }

            @Override
            public int numberOfSubscriberBeforeConnecting() {
                return -1;
            }
        }, 10);

        Multi.createFrom().publisher(emitter.getPublisher())
                .subscribe((Flow.Subscriber<? super Message<?>>) connector.getSubscriber(config));

        connector.establishQueue(channelName, new RabbitMQConnectorIncomingConfiguration(config));
    }

    @Override
    public <V> CompletionStage<Void> handle(IncomingRabbitMQMessage<V> msg, Context context, Throwable reason) {

        final int deliveryCount = (int) msg.getHeaders().getOrDefault(deliveryCountName, 0);
        final boolean lastDelivery = maxDeliveryCount.map(max -> deliveryCount >= max).orElse(false);

        if (lastDelivery) {
            return tooManyDeliveryNackHandler.handle(msg, context, reason);
        } else {
            Builder metadataBuilder = OutgoingRabbitMQMetadata.builder()
                    .withRoutingKey(reconsumeQueueName)
                    .withHeaders(msg.getHeaders())
                    .withHeader(deliveryCountName, deliveryCount + 1);

            Message<?> message = Message.of(msg.getPayload(), Metadata.of(metadataBuilder
                    .build()), () -> ackHandler.handle(msg, context), e -> nackHandler.handle(msg, context, e));
            return emitter.sendMessage(message)
                    .runSubscriptionOn(context::runOnContext)
                    .subscribeAsCompletionStage();
        }
    }

    protected RabbitMQFailureHandler createFailureHandler(String strategy, RabbitMQConnectorIncomingConfiguration config,
            RabbitMQConnector connector, Instance<RabbitMQFailureHandler.Factory> failureHandlerFactories) {
        Instance<RabbitMQFailureHandler.Factory> failureHandlerFactory = CDIUtils.getInstanceById(failureHandlerFactories,
                strategy);
        // prevent infinite recursive loop
        if (!identifier.equals(strategy) && failureHandlerFactory.isResolvable()) {
            return failureHandlerFactory.get().create(config, connector);
        } else {
            throw ex.illegalArgumentInvalidFailureStrategy(strategy);
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
