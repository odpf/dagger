package io.odpf.dagger.core.exception;

/**
 * The class Exception if failed on Serializing the protobuf message.
 */
public class DaggerSerializationException extends RuntimeException {
    /**
     * Instantiates a new Dagger serialization exception.
     *
     * @param protoClassMisconfiguredError the proto class misconfigured error
     */
    public DaggerSerializationException(String protoClassMisconfiguredError) {
        super(protoClassMisconfiguredError);
    }
}
