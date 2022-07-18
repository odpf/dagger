package io.odpf.dagger.core.source;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.metrics.reporters.statsd.SerializedStatsDReporterSupplier;
import io.odpf.dagger.common.serde.json.deserialization.JsonDeserializer;
import io.odpf.dagger.common.serde.parquet.deserialization.SimpleGroupDeserializer;
import io.odpf.dagger.common.serde.proto.deserialization.ProtoDeserializer;
import io.odpf.dagger.core.exception.InvalidDaggerSourceException;
import io.odpf.dagger.core.source.config.StreamConfig;
import io.odpf.dagger.core.source.config.models.SourceDetails;
import io.odpf.dagger.core.source.config.models.SourceName;
import io.odpf.dagger.core.source.config.models.SourceType;
import io.odpf.dagger.core.source.flinkkafkaconsumer.FlinkKafkaConsumerDaggerSource;
import io.odpf.dagger.core.source.kafka.KafkaDaggerSource;
import io.odpf.dagger.core.source.parquet.ParquetDaggerSource;
import io.odpf.depot.metrics.StatsDReporter;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.junit.Assert.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class DaggerSourceFactoryTest {
    @Mock
    private StreamConfig streamConfig;

    @Mock
    private Configuration configuration;

    @Mock
    private StatsDReporter statsDReporter;

    private final SerializedStatsDReporterSupplier statsDReporterSupplierMock = () -> statsDReporter;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
    }

    @Test
    public void shouldReturnKafkaDaggerSourceWhenConfigured() {
        ProtoDeserializer deserializer = Mockito.mock(ProtoDeserializer.class);
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.KAFKA_SOURCE, SourceType.UNBOUNDED)});
        DaggerSource<Row> daggerSource = DaggerSourceFactory.create(streamConfig, configuration, deserializer, statsDReporterSupplierMock);

        assertTrue(daggerSource instanceof KafkaDaggerSource);
    }

    @Test
    public void shouldReturnFlinkKafkaConsumerDaggerSourceWhenConfigured() {
        JsonDeserializer deserializer = Mockito.mock(JsonDeserializer.class);
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.KAFKA_CONSUMER, SourceType.UNBOUNDED)});
        DaggerSource<Row> daggerSource = DaggerSourceFactory.create(streamConfig, configuration, deserializer, statsDReporterSupplierMock);

        assertTrue(daggerSource instanceof FlinkKafkaConsumerDaggerSource);
    }

    @Test
    public void shouldReturnParquetDaggerSourceWhenConfigured() {
        SimpleGroupDeserializer deserializer = Mockito.mock(SimpleGroupDeserializer.class);
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.PARQUET_SOURCE, SourceType.BOUNDED)});
        DaggerSource<Row> daggerSource = DaggerSourceFactory.create(streamConfig, configuration, deserializer, statsDReporterSupplierMock);

        assertTrue(daggerSource instanceof ParquetDaggerSource);
    }

    @Test
    public void shouldThrowRuntimeExceptionAndReportErrorIfNoDaggerSourceCouldBeCreatedAsPerConfigs() {
        SimpleGroupDeserializer deserializer = Mockito.mock(SimpleGroupDeserializer.class);
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.PARQUET_SOURCE, SourceType.UNBOUNDED)});

        assertThrows(InvalidDaggerSourceException.class, () -> DaggerSourceFactory.create(streamConfig, configuration, deserializer, statsDReporterSupplierMock));
        verify(statsDReporter, times(1))
                .captureCount("fatal.exception", 1L, "fatal_exception_type=" + InvalidDaggerSourceException.class.getName());
    }
}
