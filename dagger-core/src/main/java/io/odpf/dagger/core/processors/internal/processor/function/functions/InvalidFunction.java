package io.odpf.dagger.core.processors.internal.processor.function.functions;

import io.odpf.dagger.core.exception.InvalidConfigurationException;

import io.odpf.dagger.core.processors.internal.processor.function.FunctionProcessor;
import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import io.odpf.dagger.core.processors.common.RowManager;
import java.io.Serializable;
public class InvalidFunction implements FunctionProcessor, Serializable {
    private InternalSourceConfig internalSourceConfig;
    private Configuration configuration;

    /**
     * Instantiates a new Invalid internal function processor.
     *
     * @param internalSourceConfig the internal source config
     * @param configuration the dagger config
     */
    public InvalidFunction(InternalSourceConfig internalSourceConfig, Configuration configuration) {
        this.internalSourceConfig = internalSourceConfig;
        this.configuration = configuration;
    }

    @Override
    public boolean canProcess(String functionName) {
        return false;
    }

    public Object getResult(RowManager rowManager) {
        String functionName = "";
        if (internalSourceConfig != null) {
            functionName = internalSourceConfig.getValue();
        }
        throw new InvalidConfigurationException(String.format("The function \"%s\" is not supported in custom configuration", functionName));
    }
}
