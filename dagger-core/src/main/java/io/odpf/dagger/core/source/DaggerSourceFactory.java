package io.odpf.dagger.core.source;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.metrics.reporters.statsd.SerializedStatsDReporterSupplier;
import io.odpf.dagger.common.serde.DaggerDeserializer;
import io.odpf.dagger.core.exception.InvalidDaggerSourceException;
import io.odpf.dagger.core.metrics.reporters.statsd.StatsDErrorReporter;
import io.odpf.dagger.core.source.config.StreamConfig;
import io.odpf.dagger.core.source.flinkkafkaconsumer.FlinkKafkaConsumerDaggerSource;
import io.odpf.dagger.core.source.kafka.KafkaDaggerSource;
import io.odpf.dagger.core.source.parquet.ParquetDaggerSource;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DaggerSourceFactory {

    public static DaggerSource<Row> create(StreamConfig streamConfig, Configuration configuration, DaggerDeserializer<Row> deserializer, SerializedStatsDReporterSupplier statsDReporterSupplier) {
        List<DaggerSource<Row>> daggerSources = getDaggerSources(streamConfig, configuration, deserializer, statsDReporterSupplier);
        return daggerSources.stream()
                .filter(DaggerSource::canBuild)
                .findFirst()
                .orElseThrow(() -> {
                    StatsDErrorReporter statsDErrorReporter = new StatsDErrorReporter(statsDReporterSupplier);
                    String sourceDetails = Arrays.toString(streamConfig.getSourceDetails());
                    InvalidDaggerSourceException ex = new InvalidDaggerSourceException(String.format("No suitable DaggerSource can be created as per SOURCE_DETAILS config %s", sourceDetails));
                    statsDErrorReporter.reportFatalException(ex);
                    return ex;
                });
    }

    private static List<DaggerSource<Row>> getDaggerSources(StreamConfig streamConfig, Configuration configuration, DaggerDeserializer<Row> deserializer, SerializedStatsDReporterSupplier statsDReporterSupplier) {
        KafkaDaggerSource kafkaDaggerSource = new KafkaDaggerSource(streamConfig, configuration, deserializer);
        FlinkKafkaConsumerDaggerSource flinkKafkaConsumerDaggerSource = new FlinkKafkaConsumerDaggerSource(streamConfig, configuration, deserializer);
        ParquetDaggerSource parquetDaggerSource = new ParquetDaggerSource(streamConfig, configuration, deserializer, statsDReporterSupplier);
        return Stream.of(kafkaDaggerSource, flinkKafkaConsumerDaggerSource, parquetDaggerSource)
                .collect(Collectors.toList());
    }
}
