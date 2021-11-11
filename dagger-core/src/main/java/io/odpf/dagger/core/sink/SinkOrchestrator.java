package io.odpf.dagger.core.sink;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducerBase;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.core.metrics.telemetry.TelemetryPublisher;
import io.odpf.dagger.core.sink.influx.ErrorHandler;
import io.odpf.dagger.core.sink.influx.InfluxDBFactoryWrapper;
import io.odpf.dagger.core.sink.influx.InfluxDBSink;
import io.odpf.dagger.core.sink.log.LogSink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.odpf.dagger.core.metrics.telemetry.TelemetryTypes.OUTPUT_PROTO;
import static io.odpf.dagger.core.metrics.telemetry.TelemetryTypes.OUTPUT_STREAM;
import static io.odpf.dagger.core.metrics.telemetry.TelemetryTypes.OUTPUT_TOPIC;
import static io.odpf.dagger.core.metrics.telemetry.TelemetryTypes.SINK_TYPE;
import static io.odpf.dagger.core.utils.Constants.*;

/**
 * The Sink orchestrator.
 * Responsible for handling the sink type.
 */
public class SinkOrchestrator implements TelemetryPublisher {

    private Map<String, List<String>> metrics = new HashMap<>();

    @Override
    public Map<String, List<String>> getTelemetry() {
        return metrics;
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
        // TODO : Convert this to enum
        String sinkType = configuration.getString("SINK_TYPE", "influx");
        addMetric(SINK_TYPE.getValue(), sinkType);
        Sink sink;
        switch (sinkType) {
            case "kafka":
                String outputTopic = configuration.getString(SINK_KAFKA_TOPIC_KEY, "");
                String outputProtoKey = configuration.getString(SINK_KAFKA_PROTO_KEY, null);
                String outputProtoMessage = configuration.getString(SINK_KAFKA_PROTO_MESSAGE_KEY, "");
                String outputStream = configuration.getString(SINK_KAFKA_STREAM_KEY, "");
                String outputBootStrapServers = configuration.getString(SINK_KAFKA_BROKERS_KEY, "");
                addMetric(OUTPUT_TOPIC.getValue(), outputTopic);
                addMetric(OUTPUT_PROTO.getValue(), outputProtoMessage);
                addMetric(OUTPUT_STREAM.getValue(), outputStream);

                ProtoSerializer recordSerializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator, outputTopic);
                sink = KafkaSink.<Row>builder()
                        .setBootstrapServers(outputBootStrapServers)
                        .setKafkaProducerConfig(getProducerProperties(configuration))
                        .setRecordSerializer(recordSerializer)
                        .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build();

                break;
            case "log":
                sink = new LogSink(columnNames);
                break;
            default:
                sink = new InfluxDBSink(new InfluxDBFactoryWrapper(), configuration, columnNames, new ErrorHandler());
        }
        notifySubscriber();
        return sink;
    }

    private void addMetric(String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
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
}
