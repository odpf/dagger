package io.odpf.dagger.sink.influx.errors;

import io.odpf.dagger.sink.influx.InfluxRowSink;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValidError implements InfluxError {

    private static final Logger LOGGER = LoggerFactory.getLogger(InfluxRowSink.class.getName());
    private Exception error;

    @Override
    public boolean hasException() {
        return true;
    }

    @Override
    public Exception getCurrentException() {
        return error;
    }

    @Override
    public boolean filterError(Throwable throwable) {
        return throwable instanceof Error;
    }

    @Override
    public void handle(Iterable<Point> points, Throwable throwable) {
        error = new Exception(throwable);
        logFailedPoints(points, LOGGER);
    }
}