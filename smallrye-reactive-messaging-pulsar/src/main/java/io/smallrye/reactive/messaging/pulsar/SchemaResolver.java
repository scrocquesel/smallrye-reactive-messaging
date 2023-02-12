package io.smallrye.reactive.messaging.pulsar;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import io.smallrye.reactive.messaging.providers.helpers.CDIUtils;

@ApplicationScoped
public class SchemaResolver {

    private final Instance<Schema<?>> schemas;

    @Inject
    public SchemaResolver(@Any Instance<Schema<?>> schemas) {
        this.schemas = schemas;
    }

    public Schema<?> getSchema(PulsarConnectorIncomingConfiguration ic) {
        return getSchema(ic, true);

    }

    public Schema<?> getSchema(PulsarConnectorOutgoingConfiguration oc) {
        return getSchema(oc, false);
    }

    private Schema<?> getSchema(PulsarConnectorCommonConfiguration configuration, boolean incoming) {
        if (configuration.getSchema().isPresent()) {
            String schemaName = configuration.getSchema().get();
            return Schema.getSchema(SchemaInfo.builder().type(SchemaType.valueOf(schemaName)).build());
        } else {
            Instance<Schema<?>> schema = CDIUtils.getInstanceById(this.schemas, configuration.getChannel());
            if (schema.isResolvable()) {
                return schema.get();
            }
        }
        return incoming ? Schema.AUTO_CONSUME() : Schema.AUTO_PRODUCE_BYTES();
    }
}
