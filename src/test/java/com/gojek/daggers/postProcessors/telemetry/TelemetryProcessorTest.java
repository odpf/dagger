package com.gojek.daggers.postProcessors.telemetry;

import com.gojek.dagger.common.StreamInfo;
import com.gojek.daggers.postProcessors.PostProcessorConfig;
import com.gojek.daggers.postProcessors.telemetry.processor.MetricsTelemetryExporter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

public class TelemetryProcessorTest {

    private TelemetryProcessor telemetryProcessor;

    private String[] columnNames = new String[]{"column_a", "column_b"};

    @Mock
    private MetricsTelemetryExporter metricsTelemetryExporter;

    @Mock
    private DataStream<Row> dataStream;

    @Mock
    private PostProcessorConfig postProcessorConfig;

    @Before
    public void setup() {
        initMocks(this);
        telemetryProcessor = new TelemetryProcessor(metricsTelemetryExporter);

    }

    @Test
    public void shouldMapMetricTelemetryExporterOnProcess() {
        StreamInfo streamInfo = new StreamInfo(dataStream, columnNames);
        telemetryProcessor.process(streamInfo);

        verify(streamInfo.getDataStream(), times(1)).map(metricsTelemetryExporter);
    }

    @Test
    public void shouldReturnTrueOnCanProcess() {
        telemetryProcessor.canProcess(postProcessorConfig);
        Assert.assertTrue(telemetryProcessor.canProcess(postProcessorConfig));
    }

}