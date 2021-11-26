package io.odpf.dagger.core.sink.kafka.builder;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.metrics.telemetry.TelemetryPublisher;
import io.odpf.dagger.core.metrics.telemetry.TelemetryTypes;
import io.odpf.dagger.core.sink.kafka.KafkaSerializerBuilder;
import io.odpf.dagger.core.utils.Constants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaJsonSerializerBuilder implements KafkaSerializerBuilder, TelemetryPublisher {
    private Map<String, List<String>> metrics;
    private Configuration configuration;

    public KafkaJsonSerializerBuilder(Configuration configuration) {
        this.configuration = configuration;
        this.metrics = new HashMap<>();
    }

    @Override
    public Map<String, List<String>> getTelemetry() {
        return metrics;
    }

    @Override
    public KafkaRecordSerializationSchema build() {
        String outputTopic = configuration.getString(Constants.SINK_KAFKA_TOPIC_KEY, "");
        String outputStream = configuration.getString(Constants.SINK_KAFKA_STREAM_KEY, "");
        String outputJsonSchema = configuration.getString(Constants.SINK_KAFKA_JSON_SCHEMA_KEY, "");
        addMetric(TelemetryTypes.OUTPUT_TOPIC.getValue(), outputTopic);
        addMetric(TelemetryTypes.OUTPUT_STREAM.getValue(), outputStream);
        //TODO : check if additional things needs to be added here
        notifySubscriber();

        TypeInformation<Row> opTypeInfo = JsonRowSchemaConverter.convert(outputJsonSchema);
        JsonRowSerializationSchema jsonRowSerializationSchema = JsonRowSerializationSchema
                .builder()
                .withTypeInfo(opTypeInfo)
                .build();
        return KafkaRecordSerializationSchema
                .builder()
                .setValueSerializationSchema(jsonRowSerializationSchema)
                .setTopic(outputTopic)
                .build();
    }

    private void addMetric(String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }
}
