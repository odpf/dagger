package io.odpf.dagger.core.sink;

import io.odpf.dagger.core.sink.bigquery.BigquerySinkBuilder;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducerBase;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.core.metrics.telemetry.TelemetryPublisher;
import io.odpf.dagger.core.metrics.telemetry.TelemetryTypes;
import io.odpf.dagger.core.processors.telemetry.processor.MetricsTelemetryExporter;
import io.odpf.dagger.core.sink.influx.ErrorHandler;
import io.odpf.dagger.core.sink.influx.InfluxDBFactoryWrapper;
import io.odpf.dagger.core.sink.influx.InfluxDBSink;
import io.odpf.dagger.core.sink.kafka.KafkaSerializationSchemaFactory;
import io.odpf.dagger.core.sink.kafka.KafkaSerializerBuilder;
import io.odpf.dagger.core.sink.log.LogSink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.odpf.dagger.core.utils.Constants.*;

/**
 * The Sink orchestrator.
 * Responsible for handling the sink type.
 */
public class SinkOrchestrator implements TelemetryPublisher {
    private final MetricsTelemetryExporter telemetryExporter;
    private final Map<String, List<String>> metrics;

    public SinkOrchestrator(MetricsTelemetryExporter telemetryExporter) {
        this.telemetryExporter = telemetryExporter;
        this.metrics = new HashMap<>();
    }

    /**
     * Gets sink.
     *
     * @return the sink
     * @configuration configuration             the configuration
     * @columnNames columnNames               the column names
     * @StencilClientOrchestrator stencilClientOrchestrator the stencil client orchestrator
     */
    public Sink getSink(Configuration configuration, String[] columnNames, StencilClientOrchestrator stencilClientOrchestrator) {
        String sinkType = configuration.getString("SINK_TYPE", "influx");
        addMetric(TelemetryTypes.SINK_TYPE.getValue(), sinkType);
        Sink sink;
        switch (sinkType) {
            case "kafka":
                String outputBootStrapServers = configuration.getString(SINK_KAFKA_BROKERS_KEY, "");

                KafkaSerializerBuilder serializationSchema = KafkaSerializationSchemaFactory
                        .getSerializationSchema(configuration, stencilClientOrchestrator, columnNames);

                reportTelemetry(serializationSchema);

                sink = KafkaSink.<Row>builder()
                        .setBootstrapServers(outputBootStrapServers)
                        .setKafkaProducerConfig(getProducerProperties(configuration))
                        .setRecordSerializer(serializationSchema.build())
                        .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build();

                break;
            case "log":
                sink = new LogSink(columnNames);
                break;
            case "bigquery":
                sink = BigquerySinkBuilder.create()
                        .setColumnNames(columnNames)
                        .setConfiguration(configuration)
                        .setStencilClientOrchestrator(stencilClientOrchestrator)
                        .build();
                break;
            default:
                sink = new InfluxDBSink(new InfluxDBFactoryWrapper(), configuration, columnNames, new ErrorHandler());
        }
        notifySubscriber();
        return sink;
    }

    private void reportTelemetry(KafkaSerializerBuilder kafkaSchemaBuilder) {
        TelemetryPublisher pub = (TelemetryPublisher) kafkaSchemaBuilder;
        pub.addSubscriber(telemetryExporter);
    }


    /**
     * Gets producer properties.
     *
     * @param configuration the configuration
     * @return the producer properties
     */
    protected Properties getProducerProperties(Configuration configuration) {
        String outputBrokerList = configuration.getString(SINK_KAFKA_BROKERS_KEY, "");
        Properties kafkaProducerConfigs = FlinkKafkaProducerBase.getPropertiesFromBrokerList(outputBrokerList);
        if (configuration.getBoolean(SINK_KAFKA_PRODUCE_LARGE_MESSAGE_ENABLE_KEY, SINK_KAFKA_PRODUCE_LARGE_MESSAGE_ENABLE_DEFAULT)) {
            kafkaProducerConfigs.setProperty(SINK_KAFKA_COMPRESSION_TYPE_KEY, SINK_KAFKA_COMPRESSION_TYPE_DEFAULT);
            kafkaProducerConfigs.setProperty(SINK_KAFKA_MAX_REQUEST_SIZE_KEY, SINK_KAFKA_MAX_REQUEST_SIZE_DEFAULT);
        }
        return kafkaProducerConfigs;
    }

    @Override
    public Map<String, List<String>> getTelemetry() {
        return metrics;
    }

    private void addMetric(String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }
}
