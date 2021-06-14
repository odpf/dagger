package io.odpf.dagger.core.metrics.reporters;

import io.odpf.dagger.core.utils.Constants;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

/**
 * The Factory class for Error reporter.
 */
public class ErrorReporterFactory {

    /**
     * Gets error reporter.
     *
     * @param runtimeContext the runtime context
     * @param configuration  the configuration
     * @return the error reporter
     */
    public static ErrorReporter getErrorReporter(RuntimeContext runtimeContext, Configuration configuration) {
        long shutDownPeriod = configuration.getLong(Constants.SHUTDOWN_PERIOD_KEY, Constants.SHUTDOWN_PERIOD_DEFAULT);
        boolean telemetryEnabled = configuration.getBoolean(Constants.TELEMETRY_ENABLED_KEY, Constants.TELEMETRY_ENABLED_VALUE_DEFAULT);
        return getErrorReporter(runtimeContext, telemetryEnabled, shutDownPeriod);
    }

    /**
     * Gets error reporter.
     *
     * @param runtimeContext  the runtime context
     * @param telemetryEnable the telemetry enable
     * @param shutDownPeriod  the shut down period
     * @return the error reporter
     */
    public static ErrorReporter getErrorReporter(RuntimeContext runtimeContext, Boolean telemetryEnable, long shutDownPeriod) {
        if (telemetryEnable) {
            return new ErrorStatsReporter(runtimeContext, shutDownPeriod);
        } else {
            return new NoOpErrorReporter();
        }
    }
}
