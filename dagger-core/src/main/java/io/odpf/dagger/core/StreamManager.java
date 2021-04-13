package io.odpf.dagger.core;

import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.metrics.telemetry.AggregatedUDFTelemetryPublisher;
import io.odpf.dagger.processors.PreProcessorConfig;
import io.odpf.dagger.processors.types.PostProcessor;
import io.odpf.dagger.processors.types.Preprocessor;
import io.odpf.dagger.processors.PostProcessorFactory;
import io.odpf.dagger.processors.telemetry.processor.MetricsTelemetryExporter;
import io.odpf.dagger.processors.PreProcessorFactory;
import io.odpf.dagger.sink.SinkOrchestrator;
import io.odpf.dagger.source.CustomStreamingTableSource;
import io.odpf.dagger.utils.Constants;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.*;
import java.util.stream.Collectors;

public class StreamManager {

    private Configuration configuration;
    private StencilClientOrchestrator stencilClientOrchestrator;
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
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
        executionEnvironment.setMaxParallelism(configuration.getInteger(Constants.MAX_PARALLELISM_KEY, Constants.MAX_PARALLELISM_DEFAULT));
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


    public StreamManager registerSourceWithPreProcessors() {
        String rowTimeAttributeName = configuration.getString("ROWTIME_ATTRIBUTE_NAME", "");
        Boolean enablePerPartitionWatermark = configuration.getBoolean("ENABLE_PER_PARTITION_WATERMARK", false);
        Long watermarkDelay = configuration.getLong("WATERMARK_DELAY_MS", 10000);
        kafkaStreams = getKafkaStreams();
        kafkaStreams.notifySubscriber(telemetryExporter);
        PreProcessorConfig preProcessorConfig = PreProcessorFactory.parseConfig(configuration);
        kafkaStreams.getStreams().forEach((tableName, kafkaConsumer) -> {
            DataStream<Row> kafkaStream = executionEnvironment.addSource(kafkaConsumer);
            StreamInfo streamInfo = new StreamInfo(kafkaStream, TableSchema.fromTypeInfo(kafkaStream.getType()).getFieldNames());
            streamInfo = addPreProcessor(streamInfo, tableName, preProcessorConfig);
            CustomStreamingTableSource tableSource = new CustomStreamingTableSource(
                    rowTimeAttributeName,
                    watermarkDelay,
                    enablePerPartitionWatermark,
                    streamInfo.getDataStream()
            );
            tableEnvironment.registerTableSource(tableName, tableSource);
        });
        return this;
    }

    private Map<String, ScalarFunction> addScalarFunctions() {
        HashMap<String, ScalarFunction> scalarFunctions = new HashMap<>();
        return scalarFunctions;
    }

    private Map<String, TableFunction> addTableFunctions() {
        HashMap<String, TableFunction> tableFunctions = new HashMap<>();
        return tableFunctions;
    }

    private List<String> getStencilUrls() {
        return Arrays.stream(configuration.getString(Constants.STENCIL_URL_KEY, Constants.STENCIL_URL_DEFAULT).split(","))
                .map(String::trim)
                .collect(Collectors.toList());
    }

    private Map<String, AggregateFunction> addAggregateFunctions() {
        HashMap<String, AggregateFunction> aggregateFunctions = new HashMap<>();
        return aggregateFunctions;
    }

    public StreamManager registerFunctions() {
        Map<String, ScalarFunction> scalarFunctions = addScalarFunctions();
        Map<String, TableFunction> tableFunctions = addTableFunctions();
        Map<String, AggregateFunction> aggregateFunctions = addAggregateFunctions();
        AggregatedUDFTelemetryPublisher udfTelemetryPublisher = new AggregatedUDFTelemetryPublisher(configuration, aggregateFunctions);
        udfTelemetryPublisher.notifySubscriber(telemetryExporter);
        scalarFunctions.forEach((scalarFunctionName, scalarUDF) -> tableEnvironment.registerFunction(scalarFunctionName, scalarUDF));
        tableFunctions.forEach((tableFunctionName, tableUDF) -> tableEnvironment.registerFunction(tableFunctionName, tableUDF));
        aggregateFunctions.forEach((aggregateFunctionName, aggregateUDF) -> tableEnvironment.registerFunction(aggregateFunctionName, aggregateUDF));
        return this;
    }

    public StreamManager registerOutputStream() {
        Table table = tableEnvironment.sqlQuery(configuration.getString(Constants.SQL_QUERY, Constants.SQL_QUERY_DEFAULT));
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
        List<PostProcessor> postProcessors = PostProcessorFactory.getPostProcessors(configuration, stencilClientOrchestrator, streamInfo.getColumnNames(), telemetryExporter);
        for (PostProcessor postProcessor : postProcessors) {
            streamInfo = postProcessor.process(streamInfo);
        }
        return streamInfo;
    }

    private StreamInfo addPreProcessor(StreamInfo streamInfo, String tableName, PreProcessorConfig preProcessorConfig) {
        List<Preprocessor> preProcessors = PreProcessorFactory.getPreProcessors(configuration, preProcessorConfig, tableName, telemetryExporter);
        for (Preprocessor preprocessor : preProcessors) {
            streamInfo = preprocessor.process(streamInfo);
        }
        return streamInfo;
    }


    private void addSink(StreamInfo streamInfo) {
        SinkOrchestrator sinkOrchestrator = new SinkOrchestrator();
        sinkOrchestrator.addSubscriber(telemetryExporter);
        streamInfo.getDataStream().addSink(sinkOrchestrator.getSink(configuration, streamInfo.getColumnNames(), stencilClientOrchestrator));
    }

    private Streams getKafkaStreams() {
        String rowTimeAttributeName = configuration.getString("ROWTIME_ATTRIBUTE_NAME", "");
        Boolean enablePerPartitionWatermark = configuration.getBoolean("ENABLE_PER_PARTITION_WATERMARK", false);
        Long watermarkDelay = configuration.getLong("WATERMARK_DELAY_MS", 10000);
        return new Streams(configuration, rowTimeAttributeName, stencilClientOrchestrator, enablePerPartitionWatermark, watermarkDelay);
    }


    private String getGcsProjectId() {
        return configuration.getString(Constants.GCS_PROJECT_ID, Constants.GCS_PROJECT_DEFAULT);
    }

    private String getGcsBucketId() {
        return configuration.getString(Constants.GCS_BUCKET_ID, Constants.GCS_BUCKET_DEFAULT);
    }
}