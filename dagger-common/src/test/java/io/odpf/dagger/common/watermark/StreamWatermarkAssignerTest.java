package io.odpf.dagger.common.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.types.Row;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

public class StreamWatermarkAssignerTest {

    @Mock
    private DataStream<Row> inputStream;


    @Mock
    private FlinkKafkaConsumer consumer;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldAssignTimestampAndWatermarksToDataStream() {
        LastColumnWatermark lastColumnWatermark = new LastColumnWatermark();
        StreamWatermarkAssigner streamWatermarkAssigner = new StreamWatermarkAssigner(lastColumnWatermark);
        streamWatermarkAssigner.assignTimeStampAndWatermark(inputStream, 10L);

        verify(inputStream, times(1)).assignTimestampsAndWatermarks(any(WatermarkStrategy.class));
    }


    @Test
    public void shouldAssignTimestampAndWatermarksToKafkaConsumer() {
        LastColumnWatermark lastColumnWatermark = new LastColumnWatermark();
        StreamWatermarkAssigner streamWatermarkAssigner = new StreamWatermarkAssigner(lastColumnWatermark);
        streamWatermarkAssigner.assignTimeStampAndWatermark(consumer, 10L);

        verify(consumer, times(1)).assignTimestampsAndWatermarks(any(WatermarkStrategy.class));
    }

    @Test
    public void shouldAssignTimestampAndWatermarksForRowTimeStrategy() {
        String[] columnNames = {"test_field", "rowtime"};
        RowtimeFieldWatermark rowtimeFieldWatermark = new RowtimeFieldWatermark(columnNames);
        StreamWatermarkAssigner streamWatermarkAssigner = new StreamWatermarkAssigner(rowtimeFieldWatermark);
        streamWatermarkAssigner.assignTimeStampAndWatermark(inputStream, 10L);

        verify(inputStream, times(1)).assignTimestampsAndWatermarks(any(WatermarkStrategy.class));
    }

}
