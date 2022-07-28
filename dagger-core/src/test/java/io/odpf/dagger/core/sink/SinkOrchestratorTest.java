package io.odpf.dagger.core.sink;

import io.odpf.dagger.core.sink.bigquery.BigquerySink;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaSink;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.core.processors.telemetry.processor.MetricsTelemetryExporter;
import io.odpf.dagger.core.sink.influx.InfluxDBSink;
import io.odpf.dagger.core.sink.log.LogSink;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.*;

import static io.odpf.dagger.common.core.Constants.*;
import static io.odpf.dagger.core.utils.Constants.SINK_KAFKA_BROKERS_KEY;
import static io.odpf.dagger.core.utils.Constants.SINK_KAFKA_PRODUCE_LARGE_MESSAGE_ENABLE_KEY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class SinkOrchestratorTest {

    private Configuration configuration;
    private StencilClientOrchestrator stencilClientOrchestrator;
    private SinkOrchestrator sinkOrchestrator;
    @Mock
    private MetricsTelemetryExporter telemetryExporter;

    @Before
    public void setup() {
        initMocks(this);
        configuration = mock(Configuration.class, withSettings().serializable());
        when(configuration.getBoolean(SCHEMA_REGISTRY_STENCIL_ENABLE_KEY, SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT);
        when(configuration.getString(SCHEMA_REGISTRY_STENCIL_URLS_KEY, SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT);

        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
        sinkOrchestrator = new SinkOrchestrator(telemetryExporter);
    }

    @Test
    public void shouldGiveInfluxSinkWhenConfiguredToUseInflux() throws Exception {
        when(configuration.getString(eq("SINK_TYPE"), anyString())).thenReturn("influx");
        Sink sinkFunction = sinkOrchestrator.getSink(configuration, new String[]{}, stencilClientOrchestrator);

        assertThat(sinkFunction, instanceOf(InfluxDBSink.class));
    }

    @Test
    public void shouldGiveLogSinkWhenConfiguredToUseLog() throws Exception {
        when(configuration.getString(eq("SINK_TYPE"), anyString())).thenReturn("log");
        Sink sinkFunction = sinkOrchestrator.getSink(configuration, new String[]{}, stencilClientOrchestrator);

        assertThat(sinkFunction, instanceOf(LogSink.class));
    }

    @Test
    public void shouldGiveInfluxWhenConfiguredToUseNothing() throws Exception {
        when(configuration.getString(eq("SINK_TYPE"), anyString())).thenReturn("");
        Sink sinkFunction = sinkOrchestrator.getSink(configuration, new String[]{}, stencilClientOrchestrator);

        assertThat(sinkFunction, instanceOf(InfluxDBSink.class));
    }

    @Test
    public void shouldSetKafkaProducerConfigurations() throws Exception {
        when(configuration.getString(eq(SINK_KAFKA_BROKERS_KEY), anyString())).thenReturn("10.200.216.87:6668");
        when(configuration.getBoolean(eq(SINK_KAFKA_PRODUCE_LARGE_MESSAGE_ENABLE_KEY), anyBoolean())).thenReturn(true);
        Properties producerProperties = sinkOrchestrator.getProducerProperties(configuration);

        assertEquals(producerProperties.getProperty("compression.type"), "snappy");
        assertEquals(producerProperties.getProperty("max.request.size"), "20971520");
    }

    @Test
    public void shouldGiveKafkaProducerWhenConfiguredToUseKafkaSink() throws Exception {
        when(configuration.getString(eq("SINK_TYPE"), anyString())).thenReturn("kafka");
        when(configuration.getString(eq("SINK_KAFKA_PROTO_MESSAGE"), anyString())).thenReturn("output_proto");
        when(configuration.getString(eq("SINK_KAFKA_BROKERS"), anyString())).thenReturn("output_broker:2667");
        when(configuration.getString(eq("SINK_KAFKA_TOPIC"), anyString())).thenReturn("output_topic");
        when(configuration.getString(eq("SINK_KAFKA_DATA_TYPE"), anyString())).thenReturn("PROTO");

        Sink sinkFunction = sinkOrchestrator.getSink(configuration, new String[]{}, stencilClientOrchestrator);

        assertThat(sinkFunction, instanceOf(KafkaSink.class));
    }

    @Test
    public void shouldReturnSinkMetrics() {
        ArrayList<String> sinkType = new ArrayList<>();
        sinkType.add("influx");
        HashMap<String, List<String>> expectedMetrics = new HashMap<>();
        expectedMetrics.put("sink_type", sinkType);

        when(configuration.getString(eq("SINK_TYPE"), anyString())).thenReturn("influx");

        sinkOrchestrator.getSink(configuration, new String[]{}, stencilClientOrchestrator);
        assertEquals(expectedMetrics, sinkOrchestrator.getTelemetry());
    }

    @Test
    public void shouldReturnBigquerySink() {
        when(configuration.getString(eq("SINK_TYPE"), anyString())).thenReturn("bigquery");
        when(configuration.getString("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "")).thenReturn("some.class");
        when(configuration.getParam()).thenReturn(ParameterTool.fromMap(Collections.emptyMap()));
        Sink sinkFunction = sinkOrchestrator.getSink(configuration, new String[]{}, stencilClientOrchestrator);
        assertThat(sinkFunction, instanceOf(BigquerySink.class));
    }
}
