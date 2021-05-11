package io.odpf.dagger.core.processors;

import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.core.processors.longbow.LongbowProcessor;
import io.odpf.dagger.core.processors.types.PostProcessor;
import io.odpf.dagger.core.processors.telemetry.TelemetryProcessor;
import io.odpf.dagger.core.processors.telemetry.processor.MetricsTelemetryExporter;
import org.apache.flink.configuration.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.List;

import static io.odpf.dagger.core.utils.Constants.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;


public class PostProcessorFactoryTest {

    @Mock
    private Configuration configuration;

    @Mock
    private StencilClientOrchestrator stencilClientOrchestrator;

    @Mock
    private MetricsTelemetryExporter metricsTelemetryExporter;

    private String[] columnNames;

    private String jsonArray = "[\n"
            + "        {\n"
            + "            \"EVENT_TIMESTAMP_FIELD_INDEX\": \"4\",\n"
            + "            \"KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"false\",\n"
            + "            \"KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\",\n"
            + "            \"KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:6667\",\n"
            + "            \"KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"flink-sql-flud-gp0330\",\n"
            + "            \"PROTO_CLASS_NAME\": \"TestLogMessage\",\n"
            + "            \"TABLE_NAME\": \"data_stream\",\n"
            + "            \"TOPIC_NAMES\": \"test-topic\"\n"
            + "        }\n"
            + "]";

    @Before
    public void setup() {
        initMocks(this);
        columnNames = new String[]{"a", "b", "longbow_duration"};
    }


    @Test
    public void shouldReturnLongbowProcessor() {
        columnNames = new String[]{"longbow_key", "longbow_data", "event_timestamp", "rowtime", "longbow_duration"};
        when(configuration.getBoolean(ASYNC_IO_ENABLED_KEY, ASYNC_IO_ENABLED_DEFAULT)).thenReturn(false);
        when(configuration.getString(SQL_QUERY, SQL_QUERY_DEFAULT)).thenReturn("select a as `longbow_key` from l");
        when(configuration.getBoolean(POST_PROCESSOR_ENABLED_KEY, POST_PROCESSOR_ENABLED_KEY_DEFAULT)).thenReturn(false);
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn(jsonArray);

        List<PostProcessor> postProcessors = PostProcessorFactory.getPostProcessors(configuration, stencilClientOrchestrator, columnNames, metricsTelemetryExporter);

        Assert.assertEquals(1, postProcessors.size());
        Assert.assertEquals(LongbowProcessor.class, postProcessors.get(0).getClass());
    }

    @Test
    public void shouldReturnParentPostProcessor() {
        when(configuration.getBoolean(ASYNC_IO_ENABLED_KEY, ASYNC_IO_ENABLED_DEFAULT)).thenReturn(false);
        when(configuration.getString(SQL_QUERY, SQL_QUERY_DEFAULT)).thenReturn("test-sql");
        when(configuration.getBoolean(POST_PROCESSOR_ENABLED_KEY, POST_PROCESSOR_ENABLED_KEY_DEFAULT)).thenReturn(true);

        List<PostProcessor> postProcessors = PostProcessorFactory.getPostProcessors(configuration, stencilClientOrchestrator, columnNames, metricsTelemetryExporter);

        Assert.assertEquals(1, postProcessors.size());
        Assert.assertEquals(ParentPostProcessor.class, postProcessors.get(0).getClass());
    }

    @Test
    public void shouldReturnTelemetryPostProcessor() {
        when(configuration.getBoolean(ASYNC_IO_ENABLED_KEY, ASYNC_IO_ENABLED_DEFAULT)).thenReturn(false);
        when(configuration.getString(SQL_QUERY, SQL_QUERY_DEFAULT)).thenReturn("test-sql");
        when(configuration.getBoolean(POST_PROCESSOR_ENABLED_KEY, POST_PROCESSOR_ENABLED_KEY_DEFAULT)).thenReturn(false);
        when(configuration.getBoolean(TELEMETRY_ENABLED_KEY, TELEMETRY_ENABLED_VALUE_DEFAULT)).thenReturn(true);

        List<PostProcessor> postProcessors = PostProcessorFactory.getPostProcessors(configuration, stencilClientOrchestrator, columnNames, metricsTelemetryExporter);

        Assert.assertEquals(1, postProcessors.size());
        Assert.assertEquals(TelemetryProcessor.class, postProcessors.get(0).getClass());
    }

    @Test
    public void shouldNotReturnAnyPostProcessor() {
        when(configuration.getBoolean(ASYNC_IO_ENABLED_KEY, ASYNC_IO_ENABLED_DEFAULT)).thenReturn(false);
        when(configuration.getString(SQL_QUERY, SQL_QUERY_DEFAULT)).thenReturn("test-sql");
        when(configuration.getBoolean(POST_PROCESSOR_ENABLED_KEY, POST_PROCESSOR_ENABLED_KEY_DEFAULT)).thenReturn(false);
        List<PostProcessor> postProcessors = PostProcessorFactory.getPostProcessors(configuration, stencilClientOrchestrator, columnNames, metricsTelemetryExporter);

        Assert.assertEquals(0, postProcessors.size());
    }
}
