package io.odpf.dagger.core.sink.bigquery;

import io.odpf.dagger.common.serde.proto.serialization.ProtoSerializer;
import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.depot.OdpfSink;
import io.odpf.depot.OdpfSinkResponse;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.*;

public class BigquerySinkWriterTest {

    @Test
    public void shouldWriteToOdpfSinkInBatches() throws IOException {
        ProtoSerializer protoSerializer = Mockito.mock(ProtoSerializer.class);
        OdpfSink sink = Mockito.mock(OdpfSink.class);
        BigquerySinkWriter bigquerySinkWriter = new BigquerySinkWriter(protoSerializer, sink, 3, null, null);
        Row row = new Row(1);
        row.setField(0, "some field");
        Mockito.when(protoSerializer.serializeKey(row)).thenReturn("test".getBytes());
        Mockito.when(protoSerializer.serializeValue(row)).thenReturn("testMessage".getBytes());
        OdpfSinkResponse response = Mockito.mock(OdpfSinkResponse.class);
        Mockito.when(response.hasErrors()).thenReturn(false);
        Mockito.when(sink.pushToSink(Mockito.anyList())).thenReturn(response);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        Mockito.verify(sink, Mockito.times(2)).pushToSink(Mockito.anyList());
        Mockito.verify(response, Mockito.times(2)).hasErrors();
    }

    @Test
    public void shouldNotWriteIfCurrentSizeIsLessThanTheBatchSize() throws IOException {
        ProtoSerializer protoSerializer = Mockito.mock(ProtoSerializer.class);
        OdpfSink sink = Mockito.mock(OdpfSink.class);
        BigquerySinkWriter bigquerySinkWriter = new BigquerySinkWriter(protoSerializer, sink, 10, null, null);
        Row row = new Row(1);
        row.setField(0, "some field");
        Mockito.when(protoSerializer.serializeKey(row)).thenReturn("test".getBytes());
        Mockito.when(protoSerializer.serializeValue(row)).thenReturn("testMessage".getBytes());
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        Mockito.verify(sink, Mockito.times(0)).pushToSink(Mockito.anyList());
    }

    @Test
    public void shouldThrowExceptionWhenSinkResponseHasErrors() throws IOException {
        ProtoSerializer protoSerializer = Mockito.mock(ProtoSerializer.class);
        OdpfSink sink = Mockito.mock(OdpfSink.class);
        ErrorReporter reporter = Mockito.mock(ErrorReporter.class);
        Set<ErrorType> errorTypesForFailing = new HashSet<ErrorType>() {{
            add(ErrorType.DESERIALIZATION_ERROR);
            add(ErrorType.SINK_4XX_ERROR);
        }};
        BigquerySinkWriter bigquerySinkWriter = new BigquerySinkWriter(protoSerializer, sink, 3, reporter, errorTypesForFailing);
        Row row = new Row(1);
        row.setField(0, "some field");
        Mockito.when(protoSerializer.serializeKey(row)).thenReturn("test".getBytes());
        Mockito.when(protoSerializer.serializeValue(row)).thenReturn("testMessage".getBytes());
        OdpfSinkResponse response = Mockito.mock(OdpfSinkResponse.class);
        Mockito.when(response.hasErrors()).thenReturn(true);
        Map<Long, ErrorInfo> errorInfoMap = new HashMap<Long, ErrorInfo>() {{
            put(1L, new ErrorInfo(new Exception("test1"), ErrorType.DESERIALIZATION_ERROR));
            put(2L, new ErrorInfo(new Exception("test2"), ErrorType.SINK_4XX_ERROR));
        }};
        Mockito.when(response.getErrors()).thenReturn(errorInfoMap);
        Mockito.when(sink.pushToSink(Mockito.anyList())).thenReturn(response);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        IOException ioException = Assert.assertThrows(IOException.class, () -> bigquerySinkWriter.write(row, null));
        Assert.assertEquals("Error occurred during writing to Bigquery", ioException.getMessage());
        Mockito.verify(sink, Mockito.times(1)).pushToSink(Mockito.anyList());
        Mockito.verify(response, Mockito.times(1)).hasErrors();
        Mockito.verify(reporter, Mockito.times(0)).reportNonFatalException(Mockito.any());
        Mockito.verify(reporter, Mockito.times(1)).reportFatalException(errorInfoMap.get(1L).getException());
        Mockito.verify(reporter, Mockito.times(1)).reportFatalException(errorInfoMap.get(2L).getException());
    }

    @Test
    public void shouldNotThrowExceptionIfErrorTypeNotConfigured() throws IOException {
        ProtoSerializer protoSerializer = Mockito.mock(ProtoSerializer.class);
        OdpfSink sink = Mockito.mock(OdpfSink.class);
        ErrorReporter reporter = Mockito.mock(ErrorReporter.class);
        Set<ErrorType> errorTypesForFailing = Collections.emptySet();
        BigquerySinkWriter bigquerySinkWriter = new BigquerySinkWriter(protoSerializer, sink, 3, reporter, errorTypesForFailing);
        Row row = new Row(1);
        row.setField(0, "some field");
        Mockito.when(protoSerializer.serializeKey(row)).thenReturn("test".getBytes());
        Mockito.when(protoSerializer.serializeValue(row)).thenReturn("testMessage".getBytes());
        OdpfSinkResponse response = Mockito.mock(OdpfSinkResponse.class);
        Mockito.when(response.hasErrors()).thenReturn(true);
        Map<Long, ErrorInfo> errorInfoMap = new HashMap<Long, ErrorInfo>() {{
            put(1L, new ErrorInfo(new Exception("test1"), ErrorType.DESERIALIZATION_ERROR));
            put(2L, new ErrorInfo(new Exception("test2"), ErrorType.SINK_4XX_ERROR));
        }};
        Mockito.when(response.getErrors()).thenReturn(errorInfoMap);
        Mockito.when(sink.pushToSink(Mockito.anyList())).thenReturn(response);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        Mockito.verify(sink, Mockito.times(1)).pushToSink(Mockito.anyList());
        Mockito.verify(response, Mockito.times(1)).hasErrors();
        Mockito.verify(reporter, Mockito.times(0)).reportFatalException(Mockito.any());
        Mockito.verify(reporter, Mockito.times(1)).reportNonFatalException(errorInfoMap.get(1L).getException());
        Mockito.verify(reporter, Mockito.times(1)).reportNonFatalException(errorInfoMap.get(2L).getException());
    }

    @Test
    public void shouldCallClose() throws Exception {
        ProtoSerializer protoSerializer = Mockito.mock(ProtoSerializer.class);
        OdpfSink sink = Mockito.mock(OdpfSink.class);
        BigquerySinkWriter bigquerySinkWriter = new BigquerySinkWriter(protoSerializer, sink, 3, null, null);
        bigquerySinkWriter.close();
        Mockito.verify(sink, Mockito.times(1)).close();
    }

    @Test
    public void shouldReportExceptionThrownFromSinkConnector() throws IOException {
        ProtoSerializer protoSerializer = Mockito.mock(ProtoSerializer.class);
        OdpfSink sink = Mockito.mock(OdpfSink.class);
        ErrorReporter reporter = Mockito.mock(ErrorReporter.class);
        Set<ErrorType> errorTypesForFailing = Collections.emptySet();
        BigquerySinkWriter bigquerySinkWriter = new BigquerySinkWriter(protoSerializer, sink, 3, reporter, errorTypesForFailing);
        Row row = new Row(1);
        row.setField(0, "some field");
        Mockito.when(protoSerializer.serializeKey(row)).thenReturn("test".getBytes());
        Mockito.when(protoSerializer.serializeValue(row)).thenReturn("testMessage".getBytes());
        OdpfSinkResponse response = Mockito.mock(OdpfSinkResponse.class);
        Mockito.when(sink.pushToSink(Mockito.anyList())).thenThrow(new RuntimeException("test"));
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        RuntimeException thrown = Assert.assertThrows(RuntimeException.class, () -> bigquerySinkWriter.write(row, null));
        Assert.assertEquals("test", thrown.getMessage());
        Mockito.verify(sink, Mockito.times(1)).pushToSink(Mockito.anyList());
        Mockito.verify(response, Mockito.times(0)).hasErrors();
        Mockito.verify(reporter, Mockito.times(1)).reportFatalException(thrown);
    }

    @Test
    public void shouldFlushWhilePrepareForCommit() throws IOException {
        ProtoSerializer protoSerializer = Mockito.mock(ProtoSerializer.class);
        OdpfSink sink = Mockito.mock(OdpfSink.class);
        BigquerySinkWriter bigquerySinkWriter = new BigquerySinkWriter(protoSerializer, sink, 3, null, null);
        Row row = new Row(1);
        row.setField(0, "some field");
        Mockito.when(protoSerializer.serializeKey(row)).thenReturn("test".getBytes());
        Mockito.when(protoSerializer.serializeValue(row)).thenReturn("testMessage".getBytes());
        OdpfSinkResponse response = Mockito.mock(OdpfSinkResponse.class);
        Mockito.when(response.hasErrors()).thenReturn(false);
        Mockito.when(sink.pushToSink(Mockito.anyList())).thenReturn(response);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.prepareCommit(true);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        Mockito.verify(sink, Mockito.times(4)).pushToSink(Mockito.anyList());
    }
}
