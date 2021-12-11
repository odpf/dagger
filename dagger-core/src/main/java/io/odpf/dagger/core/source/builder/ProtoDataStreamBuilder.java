package io.odpf.dagger.core.source.builder;

import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.DataTypes;
import io.odpf.dagger.common.serde.deserialization.proto.ProtoDeserializer;
import io.odpf.dagger.core.source.StreamConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.odpf.dagger.core.metrics.telemetry.TelemetryTypes.INPUT_PROTO;
import static io.odpf.dagger.core.utils.Constants.FLINK_ROWTIME_ATTRIBUTE_NAME_DEFAULT;
import static io.odpf.dagger.core.utils.Constants.FLINK_ROWTIME_ATTRIBUTE_NAME_KEY;

public class ProtoDataStreamBuilder extends StreamBuilder {
    private final String protoClassName;
    private final StreamConfig streamConfig;
    private StencilClientOrchestrator stencilClientOrchestrator;
    private Configuration configuration;
    private Map<String, List<String>> metrics = new HashMap<>();

    public ProtoDataStreamBuilder(StreamConfig streamConfig, StencilClientOrchestrator stencilClientOrchestrator, Configuration configuration) {
        super(streamConfig, configuration);
        this.streamConfig = streamConfig;
        this.protoClassName = streamConfig.getProtoClass();
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.configuration = configuration;
    }

    @Override
    void addTelemetry() {
        addDefaultMetrics(metrics);
        addMetric(metrics, INPUT_PROTO.getValue(), protoClassName);
    }

    @Override
    Map<String, List<String>> getMetrics() {
        return metrics;
    }

    @Override
    public boolean canBuild() {
        return getInputDataType() == DataTypes.PROTO;
    }

    @Override
    KafkaRecordDeserializationSchema getDeserializationSchema() {
        int timestampFieldIndex = Integer.parseInt(streamConfig.getEventTimestampFieldIndex());
        String rowTimeAttributeName = configuration.getString(FLINK_ROWTIME_ATTRIBUTE_NAME_KEY, FLINK_ROWTIME_ATTRIBUTE_NAME_DEFAULT);
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(protoClassName, timestampFieldIndex, rowTimeAttributeName, stencilClientOrchestrator);
        return KafkaRecordDeserializationSchema.of(protoDeserializer);
    }
}
