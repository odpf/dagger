package io.odpf.dagger.core.metrics.reporters.statsd.manager;

import io.odpf.dagger.common.metrics.aspects.Aspects;
import io.odpf.dagger.core.metrics.reporters.statsd.SerializedStatsDReporterSupplier;
import io.odpf.dagger.core.metrics.reporters.statsd.measurement.Histogram;
import io.odpf.dagger.core.metrics.reporters.statsd.tags.StatsDTag;
import io.odpf.depot.metrics.StatsDReporter;

import java.util.ArrayList;

public class DaggerHistogramManager implements MeasurementManager, Histogram {
    private final StatsDReporter statsDReporter;
    private String[] formattedTags;

    public DaggerHistogramManager(SerializedStatsDReporterSupplier statsDReporterSupplier) {
        this.statsDReporter = statsDReporterSupplier.buildStatsDReporter();
    }

    @Override
    public void register(StatsDTag[] tags) {
        ArrayList<String> tagList = new ArrayList<>();
        for (StatsDTag measurementTag : tags) {
            tagList.add(measurementTag.getFormattedTag());
        }
        this.formattedTags = tagList.toArray(new String[0]);
    }

    @Override
    public void recordValue(Aspects aspect, long value) {
        statsDReporter.captureHistogram(aspect.getValue(), value, formattedTags);
    }
}
