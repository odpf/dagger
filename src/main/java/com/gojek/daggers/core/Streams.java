package com.gojek.daggers.core;


import com.gojek.daggers.metrics.TelemetryPublisher;
import com.gojek.daggers.source.ProtoDeserializer;
import com.gojek.de.stencil.StencilClient;
import com.google.gson.Gson;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.gojek.daggers.metrics.TelemetryTypes.*;
import static com.gojek.daggers.utils.Constants.*;

public class Streams implements TelemetryPublisher {
    private static final String KAFKA_PREFIX = "kafka_consumer_config_";
    private Map<String, FlinkKafkaConsumer011<Row>> streams = new HashMap<>();
    private LinkedHashMap<String, String> protoClassForTable = new LinkedHashMap<>();
    private StencilClient stencilClient;
    private boolean enablePerPartitionWatermark;
    private long watermarkDelay;
    private Map<String, List<String>> metrics = new HashMap<>();
    private String topics;
    private String protoClassName;
    private String streamName;

    public Streams(Configuration configuration, String rowTimeAttributeName, StencilClient stencilClient, boolean enablePerPartitionWatermark, long watermarkDelay) {
        this.stencilClient = stencilClient;
        this.watermarkDelay = watermarkDelay;
        this.enablePerPartitionWatermark = enablePerPartitionWatermark;
        String jsonArrayString = configuration.getString(INPUT_STREAMS, "");
        Gson gson = new Gson();
        Map[] streamsConfig = gson.fromJson(jsonArrayString, Map[].class);
        for (Map<String, String> streamConfig : streamsConfig) {
            String tableName = streamConfig.getOrDefault(STREAM_TABLE_NAME, "");
            streams.put(tableName, getKafkaConsumer(rowTimeAttributeName, streamConfig));
        }
    }

    public Map<String, FlinkKafkaConsumer011<Row>> getStreams() {
        return streams;
    }

    public LinkedHashMap<String, String> getProtos() {
        return protoClassForTable;
    }

    @Override
    public void preProcessBeforeNotifyingSubscriber() {
        addTelemetry();
    }

    @Override
    public Map<String, List<String>> getTelemetry() {
        return metrics;
    }

    private static String parseVarName(String varName, String kafkaPrefix) {
        String[] names = varName.toLowerCase().replaceAll(kafkaPrefix, "").split("_");
        return String.join(".", names);
    }

    private FlinkKafkaConsumer011<Row> getKafkaConsumer(String rowTimeAttributeName, Map<String, String> streamConfig) {
        topics = streamConfig.getOrDefault(STREAM_TOPIC_NAMES, "");
        protoClassName = streamConfig.getOrDefault(STREAM_PROTO_CLASS_NAME, "");
        streamName = streamConfig.getOrDefault(INPUT_STREAM_NAME, "");
        String tableName = streamConfig.getOrDefault(STREAM_TABLE_NAME, "");
        protoClassForTable.put(tableName, protoClassName);
        int timestampFieldIndex = Integer.parseInt(streamConfig.getOrDefault("EVENT_TIMESTAMP_FIELD_INDEX", ""));
        Properties kafkaProps = new Properties();
        streamConfig.entrySet()
                .stream()
                .filter(e -> e.getKey().toLowerCase().startsWith(KAFKA_PREFIX))
                .forEach(e -> kafkaProps.setProperty(parseVarName(e.getKey(), KAFKA_PREFIX), e.getValue()));

        FlinkKafkaConsumer011 fc = new FlinkKafkaConsumer011<>(Pattern.compile(topics), new ProtoDeserializer(protoClassName, timestampFieldIndex, rowTimeAttributeName, stencilClient), kafkaProps);

        // https://ci.apache.org/projects/flink/flink-docs-stable/dev/event_timestamps_watermarks.html#timestamps-per-kafka-partition
        if (enablePerPartitionWatermark) {
            fc.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.of(watermarkDelay, TimeUnit.MILLISECONDS)) {
                @Override
                public long extractTimestamp(Row element) {
                    int index = element.getArity() - 1;
                    return ((Timestamp) element.getField(index)).getTime();
                }
            });
        }

        return fc;
    }

    private void addTelemetry() {
        String[] topicList = topics.split("\\|");
        Arrays.asList(topicList).forEach(topic -> addMetric(INPUT_TOPIC.getValue(), topic));
        addMetric(INPUT_PROTO.getValue(), protoClassName);
        addMetric(INPUT_STREAM.getValue(), streamName);
    }

    private void addMetric(String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }
}
