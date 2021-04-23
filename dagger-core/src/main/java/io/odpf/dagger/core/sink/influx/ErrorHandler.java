package io.odpf.dagger.core.sink.influx;

import org.apache.flink.api.common.functions.RuntimeContext;

import io.odpf.dagger.core.sink.influx.errors.LateRecordDropError;
import io.odpf.dagger.core.sink.influx.errors.InfluxError;
import io.odpf.dagger.core.sink.influx.errors.NoError;
import io.odpf.dagger.core.sink.influx.errors.ValidError;
import io.odpf.dagger.core.sink.influx.errors.ValidException;
import org.influxdb.dto.Point;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

public class ErrorHandler implements Serializable {
    private BiConsumer<Iterable<Point>, Throwable> exceptionHandler;

    private InfluxError error;

    public void init(RuntimeContext runtimeContext) {
        List<InfluxError> influxErrors = Arrays.asList(
                new LateRecordDropError(runtimeContext),
                new ValidError(),
                new ValidException());

        exceptionHandler = (points, throwable) -> {
            error = influxErrors.stream()
                    .filter(influxError -> influxError.filterError(throwable))
                    .findFirst()
                    .orElse(new NoError());
            error.handle(points, throwable);
        };
    }

    public BiConsumer<Iterable<Point>, Throwable> getExceptionHandler() {
        return exceptionHandler;
    }

    public Optional<InfluxError> getError() {
        return Optional.ofNullable(error);
    }
}
