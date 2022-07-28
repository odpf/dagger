package io.odpf.dagger.core.sink.kafka.builder;

import io.odpf.dagger.common.serde.proto.serialization.ProtoSerializer;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.proto.serialization.KafkaProtoSerializer;
import io.odpf.dagger.core.metrics.telemetry.TelemetryPublisher;
import io.odpf.dagger.core.metrics.telemetry.TelemetryTypes;
import io.odpf.dagger.core.sink.kafka.KafkaSerializerBuilder;
import io.odpf.dagger.core.utils.Constants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaProtoSerializerBuilder implements KafkaSerializerBuilder, TelemetryPublisher {
    private Map<String, List<String>> metrics;
    private Configuration configuration;
    private StencilClientOrchestrator stencilClientOrchestrator;
    private String[] columnNames;

    public KafkaProtoSerializerBuilder(Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator, String[] columnNames) {
        this.configuration = configuration;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.columnNames = columnNames;
        this.metrics = new HashMap<>();
    }

    @Override
    public KafkaRecordSerializationSchema build() {
        String outputTopic = configuration.getString(Constants.SINK_KAFKA_TOPIC_KEY, "");
        String outputProtoKey = configuration.getString(Constants.SINK_KAFKA_PROTO_KEY, null);
        String outputProtoMessage = configuration.getString(Constants.SINK_KAFKA_PROTO_MESSAGE_KEY, "");
        String outputStream = configuration.getString(Constants.SINK_KAFKA_STREAM_KEY, "");
        addMetric(TelemetryTypes.OUTPUT_TOPIC.getValue(), outputTopic);
        addMetric(TelemetryTypes.OUTPUT_PROTO.getValue(), outputProtoMessage);
        addMetric(TelemetryTypes.OUTPUT_STREAM.getValue(), outputStream);
        notifySubscriber();

        ProtoSerializer protoSerializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator);
        return new KafkaProtoSerializer(protoSerializer, outputTopic);
    }

    @Override
    public Map<String, List<String>> getTelemetry() {
        return metrics;
    }

    private void addMetric(String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }
}
