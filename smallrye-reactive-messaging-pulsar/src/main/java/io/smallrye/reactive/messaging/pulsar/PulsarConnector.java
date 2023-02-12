package io.smallrye.reactive.messaging.pulsar;

import static io.smallrye.reactive.messaging.pulsar.i18n.PulsarExceptions.ex;
import static io.smallrye.reactive.messaging.pulsar.i18n.PulsarLogging.log;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import javax.annotation.PostConstruct;
import javax.annotation.Priority;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.BeforeDestroyed;
import javax.enterprise.event.Observes;
import javax.enterprise.event.Reception;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.OutboundConnector;
import io.smallrye.reactive.messaging.health.HealthReporter;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.providers.helpers.CDIUtils;
import io.vertx.mutiny.core.Vertx;

@ApplicationScoped
@Connector(PulsarConnector.CONNECTOR_NAME)
@ConnectorAttribute(name = "client-configuration", type = "string", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "Identifier of a CDI bean that provides the default Pulsar client configuration for this channel. The channel configuration can still override any attribute. The bean must have a type of Map<String, Object> and must use the @io.smallrye.common.annotation.Identifier qualifier to set the identifier.")
@ConnectorAttribute(name = "serviceUrl", type = "string", defaultValue = "pulsar://localhost:6650", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "The service URL for the Pulsar service")
@ConnectorAttribute(name = "topic", type = "string", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "The consumed / populated Pulsar topic. If not set, the channel name is used")
@ConnectorAttribute(name = "schema", type = "string", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "The Pulsar schema type of this channel. When configured a schema is built with the given SchemaType and used for the channel. When absent, the schema is resolved searching for a CDI bean typed `Schema` qualified with `@Identifier` and the channel name. As a fallback AUTO_CONSUME or AUTO_PRODUCE are used.")

@ConnectorAttribute(name = "consumer-configuration", type = "string", direction = ConnectorAttribute.Direction.INCOMING, description = "Identifier of a CDI bean that provides the default Pulsar consumer configuration for this channel. The channel configuration can still override any attribute. The bean must have a type of Map<String, Object> and must use the @io.smallrye.common.annotation.Identifier qualifier to set the identifier.")
@ConnectorAttribute(name = "ack-strategy", type = "string", direction = ConnectorAttribute.Direction.INCOMING, description = "Specify the commit strategy to apply when a message produced from a record is acknowledged. Values can be `ack`, `cumulative`.", defaultValue = "ack")
@ConnectorAttribute(name = "failure-strategy", type = "string", direction = ConnectorAttribute.Direction.INCOMING, description = "Specify the failure strategy to apply when a message produced from a record is acknowledged negatively (nack). Values can be `fail` (default), `ignore`, `nack` or `reconsume-later", defaultValue = "nack")
@ConnectorAttribute(name = "reconsumeLater.delay", type = "long", direction = ConnectorAttribute.Direction.INCOMING, description = "Default delay for reconsume failure-strategy, in seconds", defaultValue = "3")
@ConnectorAttribute(name = "negativeAck.redeliveryBackoff", type = "string", direction = ConnectorAttribute.Direction.INCOMING, description = "Comma separated values for configuring negative ack MultiplierRedeliveryBackoff, min delay, max delay, multiplier.")
@ConnectorAttribute(name = "ackTimeout.redeliveryBackoff", type = "string", direction = ConnectorAttribute.Direction.INCOMING, description = "Comma separated values for configuring ack timeout MultiplierRedeliveryBackoff, min delay, max delay, multiplier.")
@ConnectorAttribute(name = "deadLetterPolicy.maxRedeliverCount", type = "int", direction = ConnectorAttribute.Direction.INCOMING, description = "Maximum number of times that a message will be redelivered before being sent to the dead letter topic")
@ConnectorAttribute(name = "deadLetterPolicy.deadLetterTopic", type = "string", direction = ConnectorAttribute.Direction.INCOMING, description = "Name of the dead letter topic where the failing messages will be sent")
@ConnectorAttribute(name = "deadLetterPolicy.retryLetterTopic", type = "string", direction = ConnectorAttribute.Direction.INCOMING, description = "Name of the retry topic where the failing messages will be sent")
@ConnectorAttribute(name = "deadLetterPolicy.initialSubscriptionName", type = "string", direction = ConnectorAttribute.Direction.INCOMING, description = "Name of the initial subscription name of the dead letter topic")
@ConnectorAttribute(name = "batchReceive", type = "boolean", direction = ConnectorAttribute.Direction.INCOMING, description = "Whether batch receive is used to consume messages", defaultValue = "false")

@ConnectorAttribute(name = "producer-configuration", type = "string", direction = ConnectorAttribute.Direction.OUTGOING, description = "Identifier of a CDI bean that provides the default Pulsar producer configuration for this channel. The channel configuration can still override any attribute. The bean must have a type of Map<String, Object> and must use the @io.smallrye.common.annotation.Identifier qualifier to set the identifier.")
@ConnectorAttribute(name = "maxPendingMessages", type = "int", direction = ConnectorAttribute.Direction.OUTGOING, description = "The maximum size of a queue holding pending messages, i.e messages waiting to receive an acknowledgment from a broker", defaultValue = "1000")
public class PulsarConnector implements InboundConnector, OutboundConnector, HealthReporter {

    public static final String CONNECTOR_NAME = "smallrye-pulsar";

    private final Map<String, PulsarClient> clients = new ConcurrentHashMap<>();
    private final List<PulsarOutgoingChannel<?>> outgoingChannels = new CopyOnWriteArrayList<>();
    private final List<PulsarIncomingChannel<?>> incomingChannels = new CopyOnWriteArrayList<>();

    @Inject
    private ExecutionHolder executionHolder;

    private Vertx vertx;

    @Inject
    private SchemaResolver schemaResolver;

    @Inject
    private ConfigResolver configResolver;

    @Inject
    @Any
    private Instance<PulsarAckHandler.Factory> ackHandlerFactories;

    @Inject
    @Any
    private Instance<PulsarFailureHandler.Factory> failureHandlerFactories;

    @PostConstruct
    void init() {
        this.vertx = executionHolder.vertx();
    }

    @Override
    public Publisher<? extends Message<?>> getPublisher(Config config) {
        PulsarConnectorIncomingConfiguration ic = new PulsarConnectorIncomingConfiguration(config);

        PulsarClient client = clients.computeIfAbsent(clientHash(ic), new PulsarClientCreator(configResolver, vertx, ic));

        try {
            PulsarIncomingChannel<?> channel = new PulsarIncomingChannel<>(client, vertx, schemaResolver.getSchema(ic),
                    CDIUtils.getInstanceById(ackHandlerFactories, ic.getAckStrategy()).get(),
                    CDIUtils.getInstanceById(failureHandlerFactories, ic.getFailureStrategy()).get(),
                    ic, configResolver);
            incomingChannels.add(channel);
            return channel.getPublisher();
        } catch (PulsarClientException e) {
            throw ex.illegalStateUnableToBuildConsumer(e);
        }
    }

    @Override
    public Subscriber<? extends Message<?>> getSubscriber(Config config) {
        PulsarConnectorOutgoingConfiguration oc = new PulsarConnectorOutgoingConfiguration(config);

        PulsarClient client = clients.computeIfAbsent(clientHash(oc), new PulsarClientCreator(configResolver, vertx, oc));

        try {
            PulsarOutgoingChannel<?> channel = new PulsarOutgoingChannel<>(client, schemaResolver.getSchema(oc), oc,
                    configResolver);
            outgoingChannels.add(channel);
            return channel.getSubscriber();
        } catch (PulsarClientException e) {
            throw ex.illegalStateUnableToBuildProducer(e);
        }
    }

    public void terminate(
            @Observes(notifyObserver = Reception.IF_EXISTS) @Priority(50) @BeforeDestroyed(ApplicationScoped.class) Object event) {
        incomingChannels.forEach(PulsarIncomingChannel::close);
        outgoingChannels.forEach(PulsarOutgoingChannel::close);
        for (PulsarClient client : clients.values()) {
            try {
                client.close();
            } catch (PulsarClientException e) {
                log.unableToCloseClient(e);
            }
        }
        incomingChannels.clear();
        outgoingChannels.clear();
        clients.clear();
    }

    // the idea is to share clients if possible since one PulsarClient can be used for multiple producers and consumers

    private String clientHash(PulsarConnectorCommonConfiguration pulsarConnectorCommonConfiguration) {
        return HashUtil.sha256(pulsarConnectorCommonConfiguration.getServiceUrl());
    }

    private static class PulsarClientCreator implements Function<String, PulsarClient> {
        private final ConfigResolver configResolver;
        private final Vertx vertx;
        private final PulsarConnectorCommonConfiguration configuration;

        public PulsarClientCreator(ConfigResolver configResolver, Vertx vertx,
                PulsarConnectorCommonConfiguration configuration) {
            this.configResolver = configResolver;
            this.vertx = vertx;
            this.configuration = configuration;
        }

        @Override
        public PulsarClient apply(String s) {
            try {
                ClientConfigurationData data = configResolver.getClientConf(configuration);
                return new PulsarClientImpl(data, vertx.nettyEventLoopGroup());
            } catch (PulsarClientException e) {
                throw ex.illegalStateUnableToBuildClient(e);
            }
        }
    }

}
