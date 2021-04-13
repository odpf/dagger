package io.odpf.dagger.processors.common;

import io.odpf.dagger.processors.ColumnNameManager;
import io.odpf.dagger.processors.types.MapDecorator;

import org.apache.flink.types.Row;

public class InitializationDecorator implements MapDecorator {

    private ColumnNameManager columnNameManager;

    public InitializationDecorator(ColumnNameManager columnNameManager) {
        this.columnNameManager = columnNameManager;
    }

    @Override
    public Boolean canDecorate() {
        return false;
    }

    @Override
    public Row map(Row input) {
        RowManager rowManager = new RowManager(input, columnNameManager.getOutputSize());
        return rowManager.getAll();
    }


}