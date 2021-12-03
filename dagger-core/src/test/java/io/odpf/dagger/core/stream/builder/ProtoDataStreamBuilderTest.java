package io.odpf.dagger.core.stream.builder;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;

import com.gojek.de.stencil.client.StencilClient;
import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.DataTypes;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.core.stream.Stream;
import io.odpf.dagger.core.stream.StreamConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ProtoDataStreamBuilderTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private Configuration configuration;

    @Mock
    private StencilClientOrchestrator stencilClientOrchestrator;

    @Mock
    private StreamConfig streamConfig;

    @Mock
    private StencilClient stencilClient;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldProcessProtoStream() {
        when(streamConfig.getStreamDataType()).thenReturn("PROTO");
        ProtoDataStreamBuilder protoDataStreamBuilder = new ProtoDataStreamBuilder(streamConfig, stencilClientOrchestrator, configuration);

        Assert.assertTrue(protoDataStreamBuilder.canBuild());
    }

    @Test
    public void shouldParseDataTypeFromStreamConfig() {
        when(streamConfig.getStreamDataType()).thenReturn("PROTO");
        ProtoDataStreamBuilder protoDataStreamBuilder = new ProtoDataStreamBuilder(streamConfig, stencilClientOrchestrator, configuration);

        Assert.assertEquals(DataTypes.PROTO, protoDataStreamBuilder.getInputDataType());
    }

    @Test
    public void shouldIgnoreJsonStream() {
        when(streamConfig.getStreamDataType()).thenReturn("JSON");
        ProtoDataStreamBuilder protoDataStreamBuilder = new ProtoDataStreamBuilder(streamConfig, stencilClientOrchestrator, configuration);

        Assert.assertFalse(protoDataStreamBuilder.canBuild());
    }

    @Test
    public void shouldCreateDeserializationSchema() {
        when(streamConfig.getStreamDataType()).thenReturn("PROTO");
        when(streamConfig.getProtoClass()).thenReturn("com.tests.TestMessage");
        when(streamConfig.getEventTimestampFieldIndex()).thenReturn("1");
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get("com.tests.TestMessage")).thenReturn(TestBookingLogMessage.getDescriptor());

        ProtoDataStreamBuilder protoDataStreamBuilder = new ProtoDataStreamBuilder(streamConfig, stencilClientOrchestrator, configuration);

        KafkaRecordDeserializationSchema deserializationSchema = protoDataStreamBuilder.getDeserializationSchema();

        Assert.assertTrue(deserializationSchema instanceof KafkaRecordDeserializationSchema);
    }

    @Test
    public void shouldBuildProtoStreamIfConfigured() {
        HashMap<String, String> kafkaPropMap = new HashMap<>();
        kafkaPropMap.put("group.id", "dummy-consumer-group");
        kafkaPropMap.put("bootstrap.servers", "localhost:9092");

        Properties properties = new Properties();
        properties.putAll(kafkaPropMap);

        when(streamConfig.getStreamDataType()).thenReturn("PROTO");
        when(streamConfig.getProtoClass()).thenReturn("com.tests.TestMessage");
        when(streamConfig.getEventTimestampFieldIndex()).thenReturn("1");
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get("com.tests.TestMessage")).thenReturn(TestBookingLogMessage.getDescriptor());
        when(streamConfig.getKafkaProps(any())).thenReturn(properties);
        when(streamConfig.getStartingOffset()).thenReturn(OffsetsInitializer.committedOffsets(OffsetResetStrategy.valueOf("LATEST")));
        when(streamConfig.getSchemaTable()).thenReturn("test-table");

        ProtoDataStreamBuilder protoDataStreamBuilder = new ProtoDataStreamBuilder(streamConfig, stencilClientOrchestrator, configuration);

        Stream build = protoDataStreamBuilder.build();

        Assert.assertEquals(DataTypes.PROTO, build.getInputDataType());
        Assert.assertTrue(build.getSource() instanceof KafkaSource);
        Assert.assertEquals("test-table", build.getStreamName());
    }

    @Test
    public void shouldAddMetricsSpecificToKafkaSource() {
        when(streamConfig.getKafkaTopic()).thenReturn("test-topic");
        when(streamConfig.getProtoClass()).thenReturn("test-class");
        when(streamConfig.getSourceKafkaName()).thenReturn("test-kafka");
        ProtoDataStreamBuilder protoDataStreamBuilder = new ProtoDataStreamBuilder(streamConfig, stencilClientOrchestrator, configuration);
        protoDataStreamBuilder.addTelemetry();

        Map<String, List<String>> metrics = protoDataStreamBuilder.getMetrics();

        Assert.assertEquals(Arrays.asList(new String[]{"test-topic"}), metrics.get("input_topic"));
        Assert.assertEquals(Arrays.asList(new String[]{"test-class"}), metrics.get("input_proto"));
        Assert.assertEquals(Arrays.asList(new String[]{"test-kafka"}), metrics.get("input_stream"));
    }

    @Test
    public void shouldFailToCreateStreamIfSomeConfigsAreMissing() {
        thrown.expect(NullPointerException.class);
        ProtoDataStreamBuilder protoDataStreamBuilder = new ProtoDataStreamBuilder(streamConfig, stencilClientOrchestrator, configuration);

        protoDataStreamBuilder.build();
    }
}
