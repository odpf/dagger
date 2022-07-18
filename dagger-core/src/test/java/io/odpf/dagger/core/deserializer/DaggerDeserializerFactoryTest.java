package io.odpf.dagger.core.deserializer;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.DaggerDeserializer;
import io.odpf.dagger.common.serde.json.deserialization.JsonDeserializer;
import io.odpf.dagger.common.serde.parquet.deserialization.SimpleGroupDeserializer;
import io.odpf.dagger.common.serde.proto.deserialization.ProtoDeserializer;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.core.exception.DaggerConfigurationException;
import io.odpf.dagger.core.metrics.reporters.statsd.SerializedStatsDReporterSupplier;
import io.odpf.dagger.core.source.config.models.SourceDetails;
import io.odpf.dagger.core.source.config.models.SourceName;
import io.odpf.dagger.core.source.config.models.SourceType;
import io.odpf.dagger.core.source.config.StreamConfig;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.stencil.client.StencilClient;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class DaggerDeserializerFactoryTest {

    @Mock
    private StreamConfig streamConfig;

    @Mock
    private Configuration configuration;

    @Mock
    private StencilClientOrchestrator stencilClientOrchestrator;

    @Mock
    private StencilClient stencilClient;

    private final SerializedStatsDReporterSupplier statsDReporterSupplierMock = () -> mock(StatsDReporter.class);

    @Before
    public void setUp() throws Exception {
        initMocks(this);
    }

    @Test
    public void shouldReturnJsonDeserializerWhenConfigured() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.KAFKA_CONSUMER, SourceType.UNBOUNDED)});
        when(streamConfig.getDataType()).thenReturn("JSON");
        when(streamConfig.getJsonSchema()).thenReturn("{ \"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"$id\": \"https://example.com/product.schema.json\", \"title\": \"Product\", \"description\": \"A product from Acme's catalog\", \"type\": \"object\", \"properties\": { \"id\": { \"description\": \"The unique identifier for a product\", \"type\": \"string\" }, \"time\": { \"description\": \"event timestamp of the event\", \"type\": \"string\", \"format\" : \"date-time\" } }, \"required\": [ \"id\", \"time\" ] }");

        DaggerDeserializer<Row> daggerDeserializer = DaggerDeserializerFactory.create(streamConfig, configuration, stencilClientOrchestrator, statsDReporterSupplierMock);

        assertTrue(daggerDeserializer instanceof JsonDeserializer);
    }

    @Test
    public void shouldReturnProtoDeserializerWhenConfigured() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.KAFKA_CONSUMER, SourceType.UNBOUNDED)});
        when(streamConfig.getDataType()).thenReturn("PROTO");
        when(streamConfig.getEventTimestampFieldIndex()).thenReturn("5");
        when(streamConfig.getProtoClass()).thenReturn("com.tests.TestMessage");
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get("com.tests.TestMessage")).thenReturn(TestBookingLogMessage.getDescriptor());

        DaggerDeserializer<Row> daggerDeserializer = DaggerDeserializerFactory.create(streamConfig, configuration, stencilClientOrchestrator, statsDReporterSupplierMock);

        assertTrue(daggerDeserializer instanceof ProtoDeserializer);
    }

    @Test
    public void shouldReturnSimpleGroupDeserializerWhenConfigured() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.PARQUET_SOURCE, SourceType.BOUNDED)});
        when(streamConfig.getDataType()).thenReturn("PROTO");
        when(streamConfig.getEventTimestampFieldIndex()).thenReturn("5");
        when(streamConfig.getProtoClass()).thenReturn("com.tests.TestMessage");
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get("com.tests.TestMessage")).thenReturn(TestBookingLogMessage.getDescriptor());

        DaggerDeserializer<Row> daggerDeserializer = DaggerDeserializerFactory.create(streamConfig, configuration, stencilClientOrchestrator, statsDReporterSupplierMock);

        assertTrue(daggerDeserializer instanceof SimpleGroupDeserializer);
    }

    @Test
    public void shouldThrowRuntimeExceptionIfNoDeserializerCouldBeCreatedFromConfigs() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.PARQUET_SOURCE, SourceType.BOUNDED)});
        when(streamConfig.getDataType()).thenReturn("JSON");

        assertThrows(DaggerConfigurationException.class, () -> DaggerDeserializerFactory.create(streamConfig, configuration, stencilClientOrchestrator, statsDReporterSupplierMock));
    }
}
