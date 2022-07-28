package io.odpf.dagger.core.sink.kafka.builder;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.proto.serialization.KafkaProtoSerializer;
import io.odpf.dagger.core.utils.Constants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class KafkaProtoSerializerBuilderTest {
    @Mock
    private Configuration configuration;
    @Mock
    private StencilClientOrchestrator stencilClientOrchestrator;

    @Before
    public void setup() {
        initMocks(this);
        when(configuration.getString(Constants.SINK_KAFKA_TOPIC_KEY, "")).thenReturn("test-topic");
        when(configuration.getString(Constants.SINK_KAFKA_PROTO_KEY, "")).thenReturn("test-proto-key");
        when(configuration.getString(Constants.SINK_KAFKA_PROTO_MESSAGE_KEY, "")).thenReturn("test-proto-message");
        when(configuration.getString(Constants.SINK_KAFKA_STREAM_KEY, "")).thenReturn("test-kafka");
    }

    @Test
    public void shouldCreateProtoSerializer() {
        KafkaProtoSerializerBuilder kafkaProtoSerializerBuilder = new KafkaProtoSerializerBuilder(configuration, stencilClientOrchestrator, new String[]{"test-col"});
        KafkaRecordSerializationSchema kafkaSerializerSchema = kafkaProtoSerializerBuilder.build();

        Assert.assertTrue(kafkaSerializerSchema instanceof KafkaProtoSerializer);
    }

    @Test
    public void shouldAddMetrics() {
        KafkaProtoSerializerBuilder kafkaProtoSerializerBuilder = new KafkaProtoSerializerBuilder(configuration, stencilClientOrchestrator, new String[]{"test-col"});
        kafkaProtoSerializerBuilder.build();

        Map<String, List<String>> telemetry = kafkaProtoSerializerBuilder.getTelemetry();

        Assert.assertEquals(Arrays.asList(new String[]{"test-topic"}), telemetry.get("output_topic"));
        Assert.assertEquals(Arrays.asList(new String[]{"test-proto-message"}), telemetry.get("output_proto"));
        Assert.assertEquals(Arrays.asList(new String[]{"test-kafka"}), telemetry.get("output_stream"));
    }
}
