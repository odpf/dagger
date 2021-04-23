package io.odpf.dagger.processors.transformers;

import io.odpf.dagger.common.core.Transformer;
import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.metrics.telemetry.TelemetryTypes;
import io.odpf.dagger.processors.telemetry.processor.MetricsTelemetryExporter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class TransformProcessorTest {


    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private List<TransformConfig> transfromConfigs;

    @Mock
    private StreamInfo streamInfo;

    @Mock
    private DataStream<Row> dataStream;

    @Mock
    private SingleOutputStreamOperator mappedDataStream;

    @Mock
    private Transformer transformer;

    @Mock
    private Configuration configuration;

    @Mock
    private MetricsTelemetryExporter metricsTelemetryExporter;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldThrowExceptionInCaseOfWrongClassName() {
        when(streamInfo.getDataStream()).thenReturn(dataStream);
        when(streamInfo.getColumnNames()).thenReturn(null);
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("wrongClassName");
        HashMap<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("keyField", "keystore");
        transfromConfigs = new ArrayList<>();
        transfromConfigs.add(new TransformConfig("wrongClassName", transformationArguments));

        TransformProcessor transformProcessor = new TransformProcessor(transfromConfigs, configuration);
        transformProcessor.process(streamInfo);
    }

    @Test
    public void shouldThrowExceptionInCaseOfWrongConstructorTypeSupported() {
        when(streamInfo.getDataStream()).thenReturn(dataStream);
        when(streamInfo.getColumnNames()).thenReturn(null);
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("io.odpf.dagger.processors.transformers.TransformProcessor.<init>(java.util.Map, [Ljava.lang.String;, org.apache.flink.configuration.Configuration)");
        HashMap<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("keyField", "keystore");
        transfromConfigs = new ArrayList<>();
        transfromConfigs.add(new TransformConfig("io.odpf.dagger.processors.transformers.TransformProcessor", transformationArguments));

        TransformProcessor transformProcessor = new TransformProcessor(transfromConfigs, configuration);
        transformProcessor.process(streamInfo);
    }

    @Test
    public void shouldProcessClassExtendingMapFunction() {
        when(streamInfo.getDataStream()).thenReturn(dataStream);
        when(streamInfo.getColumnNames()).thenReturn(null);
        HashMap<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("keyField", "keystore");
        transfromConfigs = new ArrayList<>();
        transfromConfigs.add(new TransformConfig("MapClass", transformationArguments));

        TransformProcessorMock transformProcessor = new TransformProcessorMock(transformer, transfromConfigs);
        transformProcessor.process(streamInfo);

        verify(transformer, times(1)).transform(streamInfo);
    }

    @Test
    public void shouldAddPostProcessorTypeMetrics() {
        when(streamInfo.getDataStream()).thenReturn(dataStream);
        when(streamInfo.getColumnNames()).thenReturn(null);
        HashMap<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("keyField", "keystore");

        ArrayList<String> postProcessorType = new ArrayList<>();
        postProcessorType.add("transform_processor");
        HashMap<String, List<String>> metrics = new HashMap<>();
        metrics.put("post_processor_type", postProcessorType);
        transfromConfigs = new ArrayList<>();
        transfromConfigs.add(new TransformConfig("MapClass", transformationArguments));

        TransformProcessorMock transformProcessorMock = new TransformProcessorMock(transformer, transfromConfigs);
        transformProcessorMock.preProcessBeforeNotifyingSubscriber();

        Assert.assertEquals(metrics, transformProcessorMock.getTelemetry());
    }

    @Test
    public void shouldAddPreProcessorTypeMetrics() {
        when(streamInfo.getDataStream()).thenReturn(dataStream);
        when(streamInfo.getColumnNames()).thenReturn(null);
        HashMap<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("keyField", "keystore");

        ArrayList<String> preProcessorType = new ArrayList<>();
        preProcessorType.add("test_table_transform_processor");
        HashMap<String, List<String>> metrics = new HashMap<>();
        metrics.put("pre_processor_type", preProcessorType);
        transfromConfigs = new ArrayList<>();
        transfromConfigs.add(new TransformConfig("MapClass", transformationArguments));

        TransformProcessorMock transformProcessorMock = new TransformProcessorMock("test_table", TelemetryTypes.PRE_PROCESSOR_TYPE, transformer, transfromConfigs);
        transformProcessorMock.preProcessBeforeNotifyingSubscriber();

        Assert.assertEquals(metrics, transformProcessorMock.getTelemetry());
    }

    @Test
    public void shouldNotifySubscribers() {
        when(streamInfo.getDataStream()).thenReturn(dataStream);
        when(streamInfo.getColumnNames()).thenReturn(null);
        HashMap<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("keyField", "keystore");

        transfromConfigs = new ArrayList<>();
        transfromConfigs.add(new TransformConfig("MapClass", transformationArguments));

        TransformProcessorMock transformProcessorMock = new TransformProcessorMock(transformer, transfromConfigs);
        transformProcessorMock.notifySubscriber(metricsTelemetryExporter);

        verify(metricsTelemetryExporter, times(1)).updated(transformProcessorMock);
    }

    @Test
    public void shouldProcessTwoPostTransformers() {
        when(streamInfo.getDataStream()).thenReturn(dataStream);
        when(streamInfo.getColumnNames()).thenReturn(null);
        when(dataStream.map(any(MapFunction.class))).thenReturn(mappedDataStream);
        transfromConfigs = new ArrayList<>();
        transfromConfigs.add(new TransformConfig("io.odpf.dagger.processors.transformers.MockTransformer", new HashMap<String, Object>() {{
            put("keyField", "keystore");
        }}));
        transfromConfigs.add(new TransformConfig("io.odpf.dagger.processors.transformers.MockTransformer", new HashMap<String, Object>() {{
            put("keyField", "keystore");
        }}));

        TransformProcessor transformProcessor = new TransformProcessor(transfromConfigs, configuration);
        transformProcessor.process(streamInfo);

        verify(mappedDataStream, times(1)).map(any());
    }

    @Test
    public void shouldProcessMultiplePostTransformers() {
        when(streamInfo.getDataStream()).thenReturn(dataStream);
        when(streamInfo.getColumnNames()).thenReturn(null);
        when(dataStream.map(any(MapFunction.class))).thenReturn(mappedDataStream);
        when(mappedDataStream.map(any(MapFunction.class))).thenReturn(mappedDataStream);
        transfromConfigs = new ArrayList<>();
        transfromConfigs.add(new TransformConfig("io.odpf.dagger.processors.transformers.MockTransformer", new HashMap<String, Object>() {{
            put("keyField", "keystore");
        }}));
        transfromConfigs.add(new TransformConfig("io.odpf.dagger.processors.transformers.MockTransformer", new HashMap<String, Object>() {{
            put("keyField", "keystore");
        }}));
        transfromConfigs.add(new TransformConfig("io.odpf.dagger.processors.transformers.MockTransformer", new HashMap<String, Object>() {{
            put("keyField", "keystore");
        }}));

        TransformProcessor transformProcessor = new TransformProcessor(transfromConfigs, configuration);
        transformProcessor.process(streamInfo);

        verify(mappedDataStream, times(2)).map(any());
    }

    @Test
    public void shouldPopulateDefaultArguments() {
        TransformConfig config = new TransformConfig("com.gojek.TestProcessor", new HashMap<String, Object>() {{
            put("test-key", "test-value");
        }});
        TransformProcessor processor = new TransformProcessor("test_table", TelemetryTypes.PRE_PROCESSOR_TYPE, Collections.singletonList(config), configuration);
        Assert.assertEquals("test_table", processor.tableName);
        Assert.assertEquals(TelemetryTypes.PRE_PROCESSOR_TYPE, processor.type);
        Assert.assertEquals(1, processor.transformConfigs.size());
        Assert.assertEquals("com.gojek.TestProcessor", processor.transformConfigs.get(0).getTransformationClass());
        Assert.assertEquals("test_table", processor.transformConfigs.get(0).getTransformationArguments().get("table_name"));
        Assert.assertEquals("test-value", processor.transformConfigs.get(0).getTransformationArguments().get("test-key"));
    }

    static class TransformerMock implements Transformer {
        @Override
        public StreamInfo transform(StreamInfo streamInfo) {
            return null;
        }
    }

    class TransformProcessorMock extends TransformProcessor {

        private Transformer mockMapFunction;

        public TransformProcessorMock(Transformer mockMapFunction, List<TransformConfig> transformConfigs) {
            super(transformConfigs, configuration);
            this.mockMapFunction = mockMapFunction;
        }

        public TransformProcessorMock(String table, TelemetryTypes type, Transformer mockMapFunction, List<TransformConfig> transformConfigs) {
            super(table, type, transformConfigs, configuration);
            this.mockMapFunction = mockMapFunction;
        }

        protected Transformer getTransformMethod(TransformConfig transformConfig, String className, String[] columnNames) {
            return this.mockMapFunction;
        }
    }

}
