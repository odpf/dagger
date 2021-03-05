package com.gojek.daggers.transformers;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;

import com.gojek.dagger.common.StreamInfo;
import com.gojek.dagger.transformer.Transformer;
import com.gojek.daggers.postprocessors.telemetry.processor.MetricsTelemetryExporter;
import com.gojek.daggers.postprocessors.transfromers.TransformConfig;
import com.gojek.daggers.postprocessors.transfromers.TransformProcessor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
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
        expectedException.expectMessage("com.gojek.daggers.postprocessors.transfromers.TransformProcessor.<init>(java.util.Map," +
                " [Ljava.lang.String;, org.apache.flink.configuration.Configuration)");
        HashMap<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("keyField", "keystore");
        transfromConfigs = new ArrayList<>();
        transfromConfigs.add(new TransformConfig("com.gojek.daggers.postprocessors.transfromers.TransformProcessor", transformationArguments));

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
        HashMap<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("keyField", "keystore");
        transformationArguments.put("keyField", "keystore");
        transfromConfigs = new ArrayList<>();
        transfromConfigs.add(new TransformConfig("com.gojek.dagger.transformer.FeatureTransformer", transformationArguments));
        transfromConfigs.add(new TransformConfig("com.gojek.dagger.transformer.ClearColumnTransformer", transformationArguments));

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
        HashMap<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("keyField", "keystore");
        transformationArguments.put("keyField", "keystore");
        transfromConfigs = new ArrayList<>();
        transfromConfigs.add(new TransformConfig("com.gojek.dagger.transformer.FeatureTransformer", transformationArguments));
        transfromConfigs.add(new TransformConfig("com.gojek.dagger.transformer.ClearColumnTransformer", transformationArguments));
        transfromConfigs.add(new TransformConfig("com.gojek.dagger.transformer.FieldToMapTransformer", transformationArguments));

        TransformProcessor transformProcessor = new TransformProcessor(transfromConfigs, configuration);
        transformProcessor.process(streamInfo);

        verify(mappedDataStream, times(2)).map(any());
    }

    class TransformProcessorMock extends TransformProcessor {

        private Transformer mockMapFunction;

        public TransformProcessorMock(Transformer mockMapFunction, List<TransformConfig> transformConfigs) {
            super(transformConfigs, configuration);
            this.mockMapFunction = mockMapFunction;
        }

        protected Transformer getTransformMethod(TransformConfig transformConfig, String className, String[] columnNames) {
            return this.mockMapFunction;
        }
    }

}
