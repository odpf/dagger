package io.odpf.dagger.sink;

import io.odpf.dagger.metrics.reporters.ErrorReporter;
import io.odpf.dagger.metrics.reporters.ErrorReporterFactory;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkKafkaProducerCustom extends RichSinkFunction<Row> implements CheckpointedFunction, CheckpointListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkKafkaProducerCustom.class.getName());
    private Configuration configuration;
    private ErrorReporter errorReporter;
    private FlinkKafkaProducer<Row> flinkKafkaProducer;

    public FlinkKafkaProducerCustom(FlinkKafkaProducer<Row> flinkKafkaProducer, Configuration configuration) {
        this.flinkKafkaProducer = flinkKafkaProducer;
        this.configuration = configuration;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        flinkKafkaProducer.open(parameters);
    }

    @Override
    public void close() throws Exception {
        flinkKafkaProducer.close();
    }

    @Override
    public void invoke(Row value, Context context) throws Exception {
        try {
            invokeBaseProducer(value, context);
            LOGGER.info("row to kafka :" + value.toString());
        } catch (Exception exception) {
            errorReporter = getErrorReporter(getRuntimeContext());
            errorReporter.reportFatalException(exception);
            throw exception;
        }
    }

    @Override
    public void notifyCheckpointComplete(long l) throws Exception {
        flinkKafkaProducer.notifyCheckpointComplete(l);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        flinkKafkaProducer.snapshotState(functionSnapshotContext);
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        flinkKafkaProducer.initializeState(functionInitializationContext);
    }

    @Override
    public IterationRuntimeContext getIterationRuntimeContext() {
        return flinkKafkaProducer.getIterationRuntimeContext();
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        return flinkKafkaProducer.getRuntimeContext();
    }

    @Override
    public void setRuntimeContext(RuntimeContext t) {
        flinkKafkaProducer.setRuntimeContext(t);
    }

    protected void invokeBaseProducer(Row value, Context context) throws Exception {
        flinkKafkaProducer.invoke(value, context);
    }

    protected ErrorReporter getErrorReporter(RuntimeContext runtimeContext) {
        return ErrorReporterFactory.getErrorReporter(runtimeContext, configuration);
    }
}