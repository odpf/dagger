package com.gojek.daggers.postprocessors.internal.processor.constant;

import com.gojek.daggers.postprocessors.common.ColumnNameManager;
import com.gojek.daggers.postprocessors.external.common.RowManager;
import com.gojek.daggers.postprocessors.internal.InternalSourceConfig;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

public class ConstantInternalConfigProcessorTest {

    @Test
    public void shouldBeAbleToProcessConstantCustomType() {
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[0], new ArrayList<>());
        ConstantInternalConfigProcessor constantConfigProcessor = new ConstantInternalConfigProcessor(columnNameManager, getCustomConfig("constant"));

        Assert.assertTrue(constantConfigProcessor.canProcess("constant"));
    }

    @Test
    public void shouldNotBeAbleToProcessSqlCustomType() {
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[0], new ArrayList<>());
        ConstantInternalConfigProcessor constantConfigProcessor = new ConstantInternalConfigProcessor(columnNameManager, getCustomConfig("sql"));

        Assert.assertFalse(constantConfigProcessor.canProcess("sql"));
    }

    @Test
    public void shouldNotBeAbleToProcessFunctionCustomType() {
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[0], new ArrayList<>());
        ConstantInternalConfigProcessor constantConfigProcessor = new ConstantInternalConfigProcessor(columnNameManager, getCustomConfig("function"));

        Assert.assertFalse(constantConfigProcessor.canProcess("function"));
    }

    @Test
    public void shouldProcessToPopulateDataAtRightIndexForRightConfiguration() {
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{"input1", "input2"}, Arrays.asList("output1", "output2", "output3"));
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("output3", "testValue", "constant");
        ConstantInternalConfigProcessor constantConfigProcessor = new ConstantInternalConfigProcessor(columnNameManager, internalSourceConfig);
        Row inputRow = new Row(2);
        Row outputRow = new Row(3);
        Row parentRow = new Row(2);
        parentRow.setField(0, inputRow);
        parentRow.setField(1, outputRow);
        RowManager rowManager = new RowManager(parentRow);

        constantConfigProcessor.process(rowManager);

        Assert.assertEquals("testValue", rowManager.getOutputData().getField(2));
    }

    private InternalSourceConfig getCustomConfig(String type) {
        return new InternalSourceConfig("field", "value", type);
    }
}
