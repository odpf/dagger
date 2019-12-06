package com.gojek.daggers.core;

import com.gojek.dagger.udf.*;
import com.gojek.dagger.udf.dart.store.RedisConfig;
import com.gojek.daggers.postProcessors.PostProcessorFactory;
import com.gojek.daggers.postProcessors.common.PostProcessor;
import com.gojek.daggers.postProcessors.telemetry.processor.MetricsTelemetryExporter;
import com.gojek.daggers.sink.SinkOrchestrator;
import com.gojek.daggers.source.KafkaProtoStreamingTableSource;
import com.gojek.de.stencil.StencilClient;
import com.gojek.de.stencil.StencilClientFactory;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.gojek.daggers.utils.Constants.MAX_PARALLELISM_DEFAULT;
import static com.gojek.daggers.utils.Constants.MAX_PARALLELISM_KEY;

public class StreamManager {

    private Configuration configuration;
    private StencilClient stencilClient;
    private StreamExecutionEnvironment executionEnvironment;
    private StreamTableEnvironment tableEnvironment;
    private Streams kafkaStreams;
    private MetricsTelemetryExporter telemetryExporter = new MetricsTelemetryExporter();

    public StreamManager(Configuration configuration, StreamExecutionEnvironment executionEnvironment, StreamTableEnvironment tableEnvironment) {
        this.configuration = configuration;
        this.executionEnvironment = executionEnvironment;
        this.tableEnvironment = tableEnvironment;
    }

    public StreamManager registerConfigs() {
        createStencilClient();
        executionEnvironment.setMaxParallelism(configuration.getInteger(MAX_PARALLELISM_KEY, MAX_PARALLELISM_DEFAULT));
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        executionEnvironment.setParallelism(configuration.getInteger("PARALLELISM", 1));
        executionEnvironment.getConfig().setAutoWatermarkInterval(configuration.getInteger("WATERMARK_INTERVAL_MS", 10000));
        executionEnvironment.getCheckpointConfig().setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);
        executionEnvironment.enableCheckpointing(configuration.getLong("CHECKPOINT_INTERVAL", 30000));
        executionEnvironment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        executionEnvironment.getCheckpointConfig().setCheckpointTimeout(configuration.getLong("CHECKPOINT_TIMEOUT", 900000));
        executionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(configuration.getLong("CHECKPOINT_MIN_PAUSE", 5000));
        executionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(configuration.getInteger("MAX_CONCURRECT_CHECKPOINTS", 1));
        executionEnvironment.getConfig().setGlobalJobParameters(configuration);

        tableEnvironment.getConfig().setIdleStateRetentionTime(Time.hours(configuration.getInteger("MIN_IDLE_STATE_RETENTION_TIME", 8)),
                Time.hours(configuration.getInteger("MAX_IDLE_STATE_RETENTION_TIME", 9)));
        return this;
    }

    public StreamManager registerSource() {
        String rowTimeAttributeName = configuration.getString("ROWTIME_ATTRIBUTE_NAME", "");
        Boolean enablePerPartitionWatermark = configuration.getBoolean("ENABLE_PER_PARTITION_WATERMARK", false);
        Long watermarkDelay = configuration.getLong("WATERMARK_DELAY_MS", 10000);
        kafkaStreams = getKafkaStreams();
        kafkaStreams.notifySubscriber(telemetryExporter);
        kafkaStreams.getStreams().forEach((tableName, kafkaConsumer) -> {
            KafkaProtoStreamingTableSource tableSource = new KafkaProtoStreamingTableSource(
                    kafkaConsumer,
                    rowTimeAttributeName,
                    watermarkDelay,
                    enablePerPartitionWatermark
            );
            tableEnvironment.registerTableSource(tableName, tableSource);
        });
        return this;
    }

    public StreamManager registerFunctions() {
        String redisServer = configuration.getString("REDIS_SERVER", "localhost");
        tableEnvironment.registerFunction("S2Id", new S2Id());
        tableEnvironment.registerFunction("GEOHASH", new GeoHash());
        tableEnvironment.registerFunction("ElementAt", new ElementAt(kafkaStreams.getProtos().entrySet().iterator().next().getValue(), stencilClient));
        tableEnvironment.registerFunction("ServiceArea", new ServiceArea());
        tableEnvironment.registerFunction("ServiceAreaId", new ServiceAreaId());
        tableEnvironment.registerFunction("DistinctCount", new DistinctCount());
        tableEnvironment.registerFunction("DistinctByCurrentStatus", new DistinctByCurrentStatus());
        tableEnvironment.registerFunction("Distance", new Distance());
        tableEnvironment.registerFunction("AppBetaUsers", new AppBetaUsers());
        tableEnvironment.registerFunction("KeyValue", new KeyValue());
        tableEnvironment.registerFunction("DartContains", DartContains.withRedisDataStore(new RedisConfig(redisServer)));
        tableEnvironment.registerFunction("DartGet", DartGet.withRedisDataStore(new RedisConfig(redisServer)));
        tableEnvironment.registerFunction("Features", new Features());
        tableEnvironment.registerFunction("TimestampFromUnix", new TimestampFromUnix());
        tableEnvironment.registerFunction("ConcurrentTransactions", new ConcurrentTransactions(7200));
        tableEnvironment.registerFunction("SecondsElapsed", new SecondsElapsed());
        tableEnvironment.registerFunction("StartOfWeek", new StartOfWeek());
        tableEnvironment.registerFunction("EndOfWeek", new EndOfWeek());
        tableEnvironment.registerFunction("StartOfMonth", new StartOfMonth());
        tableEnvironment.registerFunction("EndOfMonth", new EndOfMonth());
        tableEnvironment.registerFunction("TimeInDate", new TimeInDate());
        tableEnvironment.registerFunction("MapGet", new MapGet());
        tableEnvironment.registerFunction("DistanceAggregator", new DistanceAggregator());
        tableEnvironment.registerFunction("CollectArray", new CollectArray());
        return this;
    }

    public StreamManager registerOutputStream() {
        Table table = tableEnvironment.sqlQuery(configuration.getString("SQL_QUERY", ""));
        StreamInfo streamInfo = createStreamInfo(table);
        streamInfo = addPostProcessor(streamInfo);
        addSink(streamInfo);
        return this;
    }

    public void execute() throws Exception {
        executionEnvironment.execute(configuration.getString("FLINK_JOB_ID", "SQL Flink job"));
    }

    protected StreamInfo createStreamInfo(Table table) {
        DataStream<Row> stream = tableEnvironment
                .toRetractStream(table, Row.class)
                .filter(value -> value.f0)
                .map(value -> value.f1);
        return new StreamInfo(stream, table.getSchema().getFieldNames());
    }

    private StreamInfo addPostProcessor(StreamInfo streamInfo) {
        List<PostProcessor> postProcessors = PostProcessorFactory.getPostProcessors(configuration, stencilClient, streamInfo.getColumnNames(), telemetryExporter);
        StreamInfo postProcessedStream = streamInfo;
        for (PostProcessor postProcessor : postProcessors) {
            postProcessedStream = postProcessor.process(postProcessedStream);
        }
        return postProcessedStream;
    }

    private void addSink(StreamInfo streamInfo) {
        SinkOrchestrator sinkOrchestrator = new SinkOrchestrator();
        sinkOrchestrator.addSubscriber(telemetryExporter);
        streamInfo.getDataStream().addSink(sinkOrchestrator.getSink(configuration, streamInfo.getColumnNames(), stencilClient));
    }

    private StreamManager createStencilClient() {
        Boolean enableRemoteStencil = configuration.getBoolean("ENABLE_STENCIL_URL", false);
        List<String> stencilUrls = Arrays.stream(configuration.getString("STENCIL_URL", "").split(","))
                .map(String::trim)
                .collect(Collectors.toList());
        stencilClient = enableRemoteStencil
                ? StencilClientFactory.getClient(stencilUrls, configuration.toMap())
                : StencilClientFactory.getClient();
        return this;
    }

    private Streams getKafkaStreams() {
        String rowTimeAttributeName = configuration.getString("ROWTIME_ATTRIBUTE_NAME", "");
        Boolean enablePerPartitionWatermark = configuration.getBoolean("ENABLE_PER_PARTITION_WATERMARK", false);
        Long watermarkDelay = configuration.getLong("WATERMARK_DELAY_MS", 10000);
        return new Streams(configuration, rowTimeAttributeName, stencilClient, enablePerPartitionWatermark, watermarkDelay);
    }
}
