package io.odpf.dagger.core.processors.longbow.processor;

import io.odpf.dagger.common.serde.proto.serialization.ProtoSerializer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.metrics.managers.MeterStatsManager;
import io.odpf.dagger.core.metrics.aspects.LongbowWriterAspects;
import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.metrics.telemetry.TelemetrySubscriber;
import io.odpf.dagger.core.processors.longbow.LongbowSchema;
import io.odpf.dagger.core.processors.longbow.exceptions.LongbowWriterException;
import io.odpf.dagger.core.processors.longbow.outputRow.OutputIdentity;
import io.odpf.dagger.core.processors.longbow.outputRow.WriterOutputRow;
import io.odpf.dagger.core.processors.longbow.request.PutRequestFactory;
import io.odpf.dagger.core.processors.longbow.storage.LongbowStore;
import io.odpf.dagger.core.processors.longbow.storage.PutRequest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.threeten.bp.Duration;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class LongbowWriterTest {

    @Mock
    private Configuration configuration;

    @Mock
    private org.apache.flink.configuration.Configuration flinkInternalConfig;

    @Mock
    private ResultFuture<Row> resultFuture;

    @Mock
    private LongbowStore longBowStore;

    @Mock
    private RuntimeContext runtimeContext;

    @Mock
    private MeterStatsManager meterStatsManager;

    @Mock
    private ErrorReporter errorReporter;

    @Mock
    private ProtoSerializer protoSerializer;

    @Mock
    private TelemetrySubscriber telemetrySubscriber;

    private WriterOutputRow writerOutputRow;
    private String daggerID = "FR-DR-2116";
    private String longbowData1 = "RB-9876";
    private String longbowDuration = "1d";
    private String longbowKey = "rule123#driver444";
    private Timestamp longbowRowtime = new Timestamp(1558498933);
    private String tableId = "tableId";

    private LongbowWriter defaultLongbowWriter;
    private LongbowSchema defaultLongbowSchema;
    private PutRequestFactory putRequestFactory;


    @Before
    public void setUp() {
        initMocks(this);
        when(configuration.getString("PROCESSOR_LONGBOW_GCP_PROJECT_ID", "the-big-data-production-007"))
                .thenReturn("test-project");
        when(configuration.getString("PROCESSOR_LONGBOW_GCP_INSTANCE_ID", "de-prod")).thenReturn("test-instance");
        when(configuration.getString("FLINK_JOB_ID", "SQL Flink Job")).thenReturn(daggerID);
        when(configuration.getString("PROCESSOR_LONGBOW_DOCUMENT_DURATION", "90d")).thenReturn("90d");

        String[] columnNames = {"longbow_key", "longbow_data1", "longbow_duration", "rowtime"};
        defaultLongbowSchema = new LongbowSchema(columnNames);
        writerOutputRow = new OutputIdentity();
        putRequestFactory = new PutRequestFactory(defaultLongbowSchema, protoSerializer, tableId);
        defaultLongbowWriter = new LongbowWriter(configuration, defaultLongbowSchema, meterStatsManager, errorReporter,
                longBowStore, putRequestFactory, tableId, writerOutputRow);
        defaultLongbowWriter.setRuntimeContext(runtimeContext);
    }

    @Test
    public void shouldCreateTableWhenTableDoesNotExist() throws Exception {
        when(longBowStore.tableExists(tableId)).thenReturn(false);

        defaultLongbowWriter.open(flinkInternalConfig);

        long nintyDays = (long) 90 * 24 * 60 * 60 * 1000;
        verify(longBowStore, times(1)).tableExists(tableId);
        verify(longBowStore, times(1)).createTable(Duration.ofMillis(nintyDays), "ts", tableId);
        verify(meterStatsManager, times(1)).markEvent(LongbowWriterAspects.SUCCESS_ON_CREATE_BIGTABLE);
        verify(meterStatsManager, times(1))
                .updateHistogram(eq(LongbowWriterAspects.SUCCESS_ON_CREATE_BIGTABLE_RESPONSE_TIME), any(Long.class));
    }

    @Test
    public void shouldNotCreateTableWhenTableExist() throws Exception {
        when(longBowStore.tableExists(tableId)).thenReturn(true);

        defaultLongbowWriter.open(flinkInternalConfig);

        long nintyDays = (long) 90 * 24 * 60 * 60 * 1000;
        verify(longBowStore, times(1)).tableExists(tableId);
        verify(longBowStore, times(0)).createTable(Duration.ofMillis(nintyDays), "ts", tableId);
        verify(meterStatsManager, times(0)).markEvent(LongbowWriterAspects.SUCCESS_ON_CREATE_BIGTABLE);
        verify(meterStatsManager, times(0))
                .updateHistogram(eq(LongbowWriterAspects.SUCCESS_ON_CREATE_BIGTABLE_RESPONSE_TIME), any(Long.class));
    }

    @Test
    public void shouldWriteToBigTable() throws Exception {
        Row input = new Row(4);
        input.setField(0, longbowKey);
        input.setField(1, longbowData1);
        input.setField(2, longbowDuration);
        input.setField(3, longbowRowtime);

        when(longBowStore.tableExists(tableId)).thenReturn(true);
        when(longBowStore.put(any(PutRequest.class))).thenReturn(CompletableFuture.completedFuture(null));

        defaultLongbowWriter.open(flinkInternalConfig);
        defaultLongbowWriter.asyncInvoke(input, resultFuture);

        verify(resultFuture, times(1)).complete(Collections.singletonList(input));
        verify(meterStatsManager, times(1)).markEvent(LongbowWriterAspects.SUCCESS_ON_WRITE_DOCUMENT);
        verify(meterStatsManager, times(1))
                .updateHistogram(eq(LongbowWriterAspects.SUCCESS_ON_WRITE_DOCUMENT_RESPONSE_TIME), any(Long.class));
    }

    @Test(expected = RuntimeException.class)
    public void shouldCaptureExceptionWithStatsDManagerAndRethrowExceptionOnCreateTableFailure() throws Exception {
        long nintyDays = (long) 90 * 24 * 60 * 60 * 1000;

        when(longBowStore.tableExists(tableId)).thenReturn(false);
        doThrow(new RuntimeException()).when(longBowStore).createTable(Duration.ofMillis(nintyDays), "ts", tableId);

        defaultLongbowWriter.open(flinkInternalConfig);

        verify(meterStatsManager, times(1)).markEvent(LongbowWriterAspects.FAILURES_ON_CREATE_BIGTABLE);
        verify(meterStatsManager, times(1))
                .updateHistogram(eq(LongbowWriterAspects.FAILURES_ON_CREATE_BIGTABLE_RESPONSE_TIME), any(Long.class));
    }

    @Test
    public void shouldCaptureExceptionWithStatsDManagerOnWriteDocumentFailure() throws Exception {
        String[] columnNames = {"longbow_key", "longbow_data1", "longbow_duration", "rowtime", "longbow_data2"};
        defaultLongbowSchema = new LongbowSchema(columnNames);

        Row input = new Row(5);
        input.setField(0, longbowKey);
        input.setField(1, longbowData1);
        input.setField(2, longbowDuration);
        input.setField(3, longbowRowtime);
        String longbowData2 = "RB-4321";
        input.setField(4, longbowData2);

        when(longBowStore.tableExists(tableId)).thenReturn(true);
        when(longBowStore.put(any(PutRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> {
            throw new RuntimeException();
        }));

        CountDownLatch countDownLatch = new CountDownLatch(1);
        ResultFuture<Row> callback = new ResultFuture<Row>() {
            @Override
            public void complete(Collection<Row> result) {
                countDownLatch.countDown();
            }

            @Override
            public void completeExceptionally(Throwable error) {
            }
        };
        defaultLongbowWriter.open(flinkInternalConfig);
        defaultLongbowWriter.asyncInvoke(input, callback);
        countDownLatch.await();
        verify(errorReporter, times(1)).reportNonFatalException(any(LongbowWriterException.class));
        verify(meterStatsManager, times(1)).markEvent(LongbowWriterAspects.FAILED_ON_WRITE_DOCUMENT);
        verify(meterStatsManager, times(1))
                .updateHistogram(eq(LongbowWriterAspects.FAILED_ON_WRITE_DOCUMENT_RESPONSE_TIME), any(Long.class));
    }

    @Test
    public void shouldCaptureTimeoutWithStatsDManager() throws Exception {
        String[] columnNames = {"longbow_key", "longbow_data1", "longbow_duration", "rowtime", "longbow_data2"};
        defaultLongbowSchema = new LongbowSchema(columnNames);

        Row input = new Row(5);
        input.setField(0, longbowKey);
        input.setField(1, longbowData1);
        input.setField(2, longbowDuration);
        input.setField(3, longbowRowtime);
        String longbowData2 = "RB-4321";
        input.setField(4, longbowData2);

        when(longBowStore.tableExists(tableId)).thenReturn(true);
        when(longBowStore.put(any(PutRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> {
            throw new RuntimeException();
        }));

        defaultLongbowWriter.timeout(new Row(1), resultFuture);

        verify(meterStatsManager, times(1)).markEvent(LongbowWriterAspects.TIMEOUTS_ON_WRITER);
        verify(errorReporter, times(1)).reportFatalException(any(TimeoutException.class));
    }

    @Test
    public void shouldAddPostProcessorTypeMetrics() {
        ArrayList<String> postProcessorType = new ArrayList<>();
        postProcessorType.add("longbow_writer_processor");
        HashMap<String, List<String>> metrics = new HashMap<>();
        metrics.put("post_processor_type", postProcessorType);

        String[] columnNames = {"longbow_key", "longbow_data1", "longbow_duration", "rowtime", "longbow_data2"};
        LongbowSchema longBowSchema = new LongbowSchema(columnNames);
        putRequestFactory = new PutRequestFactory(longBowSchema, protoSerializer, tableId);
        LongbowWriter longBowWriter = new LongbowWriter(configuration, longBowSchema, meterStatsManager, errorReporter,
                longBowStore, putRequestFactory, tableId, writerOutputRow);

        longBowWriter.preProcessBeforeNotifyingSubscriber();
        assertEquals(metrics, longBowWriter.getTelemetry());
    }

    @Test
    public void shouldNotifySubscribers() {
        String[] columnNames = {"longbow_key", "longbow_data1", "longbow_duration", "rowtime", "longbow_data2"};
        LongbowSchema longBowSchema = new LongbowSchema(columnNames);
        putRequestFactory = new PutRequestFactory(longBowSchema, protoSerializer, tableId);
        LongbowWriter longBowWriter = new LongbowWriter(configuration, longBowSchema, meterStatsManager, errorReporter,
                longBowStore, putRequestFactory, tableId, writerOutputRow);
        longBowWriter.notifySubscriber(telemetrySubscriber);

        verify(telemetrySubscriber, times(1)).updated(longBowWriter);
    }

    @Test
    public void shouldCloseLongbowStoreAndNotifyWhenClose() throws Exception {
        String[] columnNames = {"longbow_key", "longbow_data1", "longbow_duration", "rowtime", "longbow_data2"};
        LongbowSchema longBowSchema = new LongbowSchema(columnNames);
        putRequestFactory = new PutRequestFactory(longBowSchema, protoSerializer, tableId);
        LongbowWriter longBowWriter = new LongbowWriter(configuration, longBowSchema, meterStatsManager, errorReporter,
                longBowStore, putRequestFactory, tableId, writerOutputRow);
        longBowWriter.close();
        verify(longBowStore, times(1)).close();
        verify(meterStatsManager, times(1)).markEvent(LongbowWriterAspects.CLOSE_CONNECTION_ON_WRITER);
    }
}
