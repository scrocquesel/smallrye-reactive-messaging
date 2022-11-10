package io.smallrye.reactive.messaging.pulsar;

import static io.smallrye.reactive.messaging.pulsar.i18n.PulsarLogging.log;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Flow;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Subscriber;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;

public class PulsarOutgoingChannel<T> {

    private final Producer<T> producer;
    private final PulsarSenderProcessor processor;
    private final Flow.Subscriber<? extends Message<?>> subscriber;
    private final String channel;

    public PulsarOutgoingChannel(PulsarClient client, Schema<T> schema, PulsarConnectorOutgoingConfiguration oc,
            ConfigResolver configResolver) throws PulsarClientException {
        this.channel = oc.getChannel();
        ProducerConfigurationData conf = configResolver.getProducerConf(oc);
        if (conf.getProducerName() == null) {
            conf.setProducerName(channel);
        }
        if (conf.getTopicName() == null) {
            conf.setTopicName(oc.getTopic().orElse(channel));
        }
        Map<String, Object> producerConf = configResolver.configToMap(conf);
        ProducerBuilder<T> builder = client.newProducer(schema)
                .loadConf(producerConf);
        if (conf.getCryptoKeyReader() != null) {
            builder.cryptoKeyReader(conf.getCryptoKeyReader());
        }
        this.producer = builder.create();
        log.createdProducerWithConfig(channel, conf);
        long requests = oc.getMaxPendingMessages();
        if (requests <= 0) {
            // TODO: should this be disallowed?
            requests = Long.MAX_VALUE;
        }

        processor = new PulsarSenderProcessor(requests, true, this::sendMessage);
        subscriber = MultiUtils.via(processor, m -> m.onFailure().invoke(log::unableToDispatch));
    }

    private Uni<Void> sendMessage(Message<?> message) {
        return Uni.createFrom().item(message)
                // TODO: we need some intelligence regarding the payload type
                .onItem().transform(m -> toMessageBuilder(m, producer).value((T) m.getPayload()))
                // TODO Furthermore, we need to figure out if sendAsync should block when the queue is full, or throw an exception
                // TODO ogu : with PulsarSenderProcessor configured for maximum maxPendingMessages requsts we should not block/throw. I went for block=true
                .onItem().transformToUni(mb -> Uni.createFrom().completionStage(mb.sendAsync()))
                .onItemOrFailure().transformToUni((mid, t) -> {
                    if (t == null) {
                        return Uni.createFrom().completionStage(message.ack());
                    } else {
                        return Uni.createFrom().completionStage(message.nack(t));
                    }
                });
        // TODO ogu : handle retries
        // TODO: what thread is the subscription supposed to run on?
    }

    private TypedMessageBuilder<T> toMessageBuilder(Message<?> message, Producer<T> producer) {
        Optional<PulsarOutgoingMessageMetadata> optionalMetadata = message.getMetadata(PulsarOutgoingMessageMetadata.class);
        if (optionalMetadata.isPresent()) {
            PulsarOutgoingMessageMetadata metadata = optionalMetadata.get();
            Transaction transaction = metadata.getTransaction();
            TypedMessageBuilder<T> messageBuilder = transaction != null ? producer.newMessage(transaction)
                    : producer.newMessage();

            if (metadata.hasKey()) {
                if (metadata.getKeyBytes() != null) {
                    messageBuilder.keyBytes(metadata.getKeyBytes());
                } else {
                    messageBuilder.key(metadata.getKey());
                }
            }
            if (metadata.getOrderingKey() != null) {
                messageBuilder.orderingKey(metadata.getOrderingKey());
            }
            if (metadata.getProperties() != null) {
                messageBuilder.properties(metadata.getProperties());
            }
            if (metadata.getReplicatedClusters() != null) {
                messageBuilder.replicationClusters(metadata.getReplicatedClusters());
            }
            if (metadata.getReplicationDisabled() != null) {
                messageBuilder.disableReplication();
            }
            if (metadata.getEventTime() != null) {
                messageBuilder.eventTime(metadata.getEventTime());
            }
            if (metadata.getSequenceId() != null) {
                messageBuilder.sequenceId(metadata.getSequenceId());
            }
            if (metadata.getDeliverAt() != null) {
                messageBuilder.deliverAt(metadata.getDeliverAt());
            }
            return messageBuilder;
        } else {
            return producer.newMessage();
        }
    }

    public Flow.Subscriber<? extends Message<?>> getSubscriber() {
        return subscriber;
    }

    public String getChannel() {
        return channel;
    }

    public Producer<T> getProducer() {
        return producer;
    }

    public void close() {
        if (processor != null) {
            processor.cancel();
        }
        try {
            producer.close();
        } catch (PulsarClientException e) {
            log.unableToCloseProducer(e);
        }
    }
}
