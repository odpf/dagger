package io.odpf.dagger.core.processors.internal.processor;


import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import io.odpf.dagger.core.processors.internal.processor.sql.SqlConfigTypePathParser;
import io.odpf.dagger.core.processors.internal.processor.constant.ConstantInternalConfigProcessor;
import io.odpf.dagger.core.processors.internal.processor.function.FunctionInternalConfigProcessor;
import io.odpf.dagger.core.processors.internal.processor.invalid.InvalidInternalConfigProcessor;
import io.odpf.dagger.core.processors.internal.processor.sql.fields.SqlInternalConfigProcessor;

import java.util.Arrays;
import java.util.List;

/**
 * The factory class for Internal config handler.
 */
public class InternalConfigHandlerFactory {
    private static List<InternalConfigProcessor> getHandlers(ColumnNameManager columnNameManager, SqlConfigTypePathParser sqlPathParser, InternalSourceConfig internalSourceConfig, Configuration configuration) {
        return Arrays.asList(new SqlInternalConfigProcessor(columnNameManager, sqlPathParser, internalSourceConfig),
                new FunctionInternalConfigProcessor(columnNameManager, internalSourceConfig, configuration),
                new ConstantInternalConfigProcessor(columnNameManager, internalSourceConfig));
    }

    /**
     * Gets processor.
     *
     * @param internalSourceConfig the internal source config
     * @param columnNameManager    the column name manager
     * @param sqlPathParser        the sql path parser
     * @return the processor
     */
    public static InternalConfigProcessor getProcessor(InternalSourceConfig internalSourceConfig, ColumnNameManager columnNameManager, SqlConfigTypePathParser sqlPathParser, Configuration configuration) {
        return getHandlers(columnNameManager, sqlPathParser, internalSourceConfig, configuration)
                .stream()
                .filter(customConfigProcessor -> customConfigProcessor.canProcess(internalSourceConfig.getType()))
                .findFirst()
                .orElse(new InvalidInternalConfigProcessor(internalSourceConfig));
    }
}
