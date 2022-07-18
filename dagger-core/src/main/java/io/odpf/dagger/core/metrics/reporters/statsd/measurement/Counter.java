package io.odpf.dagger.core.metrics.reporters.statsd.measurement;

import io.odpf.dagger.common.metrics.aspects.Aspects;

import java.io.Serializable;

public interface Counter extends Serializable {
    void increment(Aspects aspect);

    void increment(Aspects aspect, long num);

    void decrement(Aspects aspect);

    void decrement(Aspects aspect, long num);
}
