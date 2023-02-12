package io.smallrye.reactive.messaging.pulsar;

import static io.smallrye.reactive.messaging.pulsar.i18n.PulsarLogging.log;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RedeliveryBackoff;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MultiplierRedeliveryBackoff;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.providers.locals.ContextOperator;
import io.vertx.core.impl.EventLoopContext;
import io.vertx.core.impl.VertxInternal;
import io.vertx.mutiny.core.Vertx;

public class PulsarIncomingChannel<T> {

    private final Consumer<T> consumer;
    private final Publisher<? extends Message<?>> publisher;
    private final String channel;
    private final PulsarAckHandler ackHandler;
    private final PulsarFailureHandler failureHandler;
    private final EventLoopContext context;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public PulsarIncomingChannel(PulsarClient client, Vertx vertx, Schema<T> schema,
            PulsarAckHandler.Factory ackHandlerFactory,
            PulsarFailureHandler.Factory failureHandlerFactory,
            PulsarConnectorIncomingConfiguration ic,
            ConfigResolver configResolver) throws PulsarClientException {
        this.channel = ic.getChannel();
        ConsumerBuilder<T> builder = client.newConsumer(schema);
        ConsumerConfigurationData<?> conf = configResolver.getConsumerConf(ic);
        if (conf.getSubscriptionName() == null) {
            String s = UUID.randomUUID().toString();
            log.noSubscriptionName(s);
            conf.setSubscriptionName(s);
        }
        if (hasTopicConfig(conf)) {
            conf.setTopicNames(Arrays.stream(ic.getTopic().orElse(channel).split(",")).collect(Collectors.toSet()));
        }
        if (conf.getConsumerName() == null) {
            conf.setConsumerName(channel);
        }
        builder.loadConf(configResolver.configToMap(conf));
        ic.getDeadLetterPolicyMaxRedeliverCount().ifPresent(i -> builder.deadLetterPolicy(getDeadLetterPolicy(ic, i)));
        ic.getNegativeAckRedeliveryBackoff().ifPresent(s -> builder.negativeAckRedeliveryBackoff(parseBackoff(s)));
        ic.getAckTimeoutRedeliveryBackoff().ifPresent(s -> builder.ackTimeoutRedeliveryBackoff(parseBackoff(s)));
        if (conf.getConsumerEventListener() != null) {
            builder.consumerEventListener(conf.getConsumerEventListener());
        }
        if (conf.getPayloadProcessor() != null) {
            builder.messagePayloadProcessor(conf.getPayloadProcessor());
        }
        if (conf.getKeySharedPolicy() != null) {
            builder.keySharedPolicy(conf.getKeySharedPolicy());
        }
        if (conf.getCryptoKeyReader() != null) {
            builder.cryptoKeyReader(conf.getCryptoKeyReader());
        }
        if (conf.getMessageCrypto() != null) {
            builder.messageCrypto(conf.getMessageCrypto());
        }
        if (ic.getBatchReceive() && conf.getBatchReceivePolicy() == null) {
            builder.batchReceivePolicy(BatchReceivePolicy.DEFAULT_POLICY);
        }

        this.consumer = builder.subscribe();
        log.createdConsumerWithConfig(channel, conf);
        this.ackHandler = ackHandlerFactory.create(consumer, ic);
        this.failureHandler = failureHandlerFactory.create(consumer, ic);
        this.context = ((VertxInternal) vertx.getDelegate()).createEventLoopContext();
        Multi<PulsarIncomingMessage<T>> receiveMulti;
        if (!ic.getBatchReceive()) {
            receiveMulti = Multi.createBy().repeating().completionStage(consumer::receiveAsync)
                    .until(m -> closed.get())
                    .emitOn(command -> context.runOnContext(event -> command.run()))
                    .onItem().transform(message -> new PulsarIncomingMessage<>(message, ackHandler, failureHandler))
                    .onFailure(throwable -> isEndOfStream(client, throwable)).recoverWithCompletion()
                    .onFailure().invoke(failure -> log.failedToReceiveFromConsumer(channel, failure));
        } else {
            receiveMulti = Multi.createBy().repeating().completionStage(consumer::batchReceiveAsync)
                    .until(m -> closed.get())
                    .emitOn(command -> context.runOnContext(event -> command.run()))
                    .onItem().transformToMultiAndConcatenate(messages -> Multi.createFrom().iterable(messages)
                            .map(message -> new PulsarIncomingMessage<>(message, ackHandler, failureHandler)))
                    .onFailure(throwable -> isEndOfStream(client, throwable)).recoverWithCompletion()
                    .onFailure().invoke(failure -> log.failedToReceiveFromConsumer(channel, failure));
        }
        this.publisher = receiveMulti
                .emitOn(context.nettyEventLoop())
                .plug(ContextOperator::apply);
    }

    private boolean isEndOfStream(PulsarClient client, Throwable throwable) {
        if (closed.get()) {
            return true;
        } else if (consumer.hasReachedEndOfTopic()) {
            log.consumerReachedEndOfTopic(channel);
            return true;
        } else if (client.isClosed()) {
            log.clientClosed(channel, throwable);
            return true;
        }
        return false;
    }

    private static DeadLetterPolicy getDeadLetterPolicy(PulsarConnectorIncomingConfiguration ic, Integer redeliverCount) {
        return DeadLetterPolicy.builder()
                .maxRedeliverCount(redeliverCount)
                .deadLetterTopic(ic.getDeadLetterPolicyDeadLetterTopic().orElse(null))
                .retryLetterTopic(ic.getDeadLetterPolicyRetryLetterTopic().orElse(null))
                .initialSubscriptionName(ic.getDeadLetterPolicyInitialSubscriptionName().orElse(null))
                .build();
    }

    private RedeliveryBackoff parseBackoff(String backoffString) {
        String[] strings = backoffString.split(",");
        try {
            return MultiplierRedeliveryBackoff.builder()
                    .minDelayMs(Long.parseLong(strings[0]))
                    .maxDelayMs(Long.parseLong(strings[1]))
                    .multiplier(Double.parseDouble(strings[2]))
                    .build();
        } catch (Exception e) {
            log.unableToParseRedeliveryBackoff(backoffString, this.channel);
            return null;
        }
    }

    private static boolean hasTopicConfig(ConsumerConfigurationData<?> conf) {
        return conf.getTopicsPattern() != null
                || (conf.getTopicNames() != null && conf.getTopicNames().isEmpty());
    }

    public Publisher<? extends Message<?>> getPublisher() {
        return publisher;
    }

    public String getChannel() {
        return channel;
    }

    public Consumer<T> getConsumer() {
        return consumer;
    }

    public void close() {
        closed.set(true);
        try {
            consumer.close();
        } catch (PulsarClientException e) {
            log.unableToCloseConsumer(e);
        }
    }

}
