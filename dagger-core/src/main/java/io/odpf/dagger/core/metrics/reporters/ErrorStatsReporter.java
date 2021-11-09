package io.odpf.dagger.core.metrics.reporters;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;

import io.odpf.dagger.core.processors.telemetry.processor.MetricsTelemetryExporter;
import io.odpf.dagger.core.utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Error stats reporter.
 */
public class ErrorStatsReporter implements ErrorReporter {
    private RuntimeContext runtimeContext;
    private long shutDownPeriod;
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsTelemetryExporter.class.getName());


    /**
     * Instantiates a new Error stats reporter.
     *
     * @param runtimeContext the runtime context
     * @param shutDownPeriod the shut down period
     */
    public ErrorStatsReporter(RuntimeContext runtimeContext, long shutDownPeriod) {
        this.runtimeContext = runtimeContext;
        this.shutDownPeriod = shutDownPeriod;
    }

    @Override
    public void reportFatalException(Exception exception) {
        Counter counter = addExceptionToCounter(exception, runtimeContext.getMetricGroup(), Constants.FATAL_EXCEPTION_METRIC_GROUP_KEY);
        counter.inc();
        try {
            Thread.sleep(shutDownPeriod);
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
        }
    }

    @Override
    public void reportNonFatalException(Exception exception) {
        Counter counter = addExceptionToCounter(exception, runtimeContext.getMetricGroup(), Constants.NONFATAL_EXCEPTION_METRIC_GROUP_KEY);
        counter.inc();
    }
}
