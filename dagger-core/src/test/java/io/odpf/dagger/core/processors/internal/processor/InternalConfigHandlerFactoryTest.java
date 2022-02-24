package io.odpf.dagger.core.processors.internal.processor;

import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import io.odpf.dagger.core.processors.internal.processor.constant.ConstantInternalConfigProcessor;
import io.odpf.dagger.core.processors.internal.processor.function.FunctionInternalConfigProcessor;
import io.odpf.dagger.core.processors.internal.processor.invalid.InvalidInternalConfigProcessor;
import io.odpf.dagger.core.processors.internal.processor.sql.fields.SqlInternalConfigProcessor;
import org.junit.Test;
import org.junit.Ignore;

import static org.junit.Assert.*;

public class InternalConfigHandlerFactoryTest {
    @Ignore("temp ignore")
    @Test
    public void shouldGetConstantInternalProcessor() {
        InternalConfigProcessor processor = InternalConfigHandlerFactory.getProcessor(new InternalSourceConfig("output_field", "value", "constant"), null, null, null);

        assertEquals(ConstantInternalConfigProcessor.class, processor.getClass());
    }

    @Ignore("temp ignore")
    @Test
    public void shouldGetFunctionInternalProcessor() {
        InternalConfigProcessor processor = InternalConfigHandlerFactory.getProcessor(new InternalSourceConfig("output_field", "functionValue", "function"), null, null, null);

        assertEquals(FunctionInternalConfigProcessor.class, processor.getClass());
    }

    @Ignore("temp ignore")
    @Test
    public void shouldGetSqlInternalProcessor() {
        InternalConfigProcessor processor = InternalConfigHandlerFactory.getProcessor(new InternalSourceConfig("output_field", "functionValue", "sql"), null, null, null);

        assertEquals(SqlInternalConfigProcessor.class, processor.getClass());
    }

    @Ignore("temp ignore")
    @Test
    public void shouldGetInvalidInternalProcessor() {
        InternalConfigProcessor processor = InternalConfigHandlerFactory.getProcessor(new InternalSourceConfig("output_field", "functionValue", "invalid"), null, null, null);

        assertEquals(InvalidInternalConfigProcessor.class, processor.getClass());
    }
}
