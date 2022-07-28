package io.odpf.dagger.core.exception;

import java.io.IOException;

public class BigqueryWriterException extends IOException {

    public BigqueryWriterException(String message, Throwable cause) {
        super(message, cause);
    }

    public BigqueryWriterException(String message) {
        super(message);
    }
}
