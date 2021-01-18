package com.gojek.daggers.postProcessors.internal.processor.sql;

import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.internal.InternalSourceConfig;

import static com.gojek.daggers.utils.Constants.*;

public class SqlInternalFieldFactory {
    private ColumnNameManager columnNameManager;
    private SqlConfigTypePathParser sqlPathParser;
    private InternalSourceConfig internalSourceConfig;

    public SqlInternalFieldFactory(ColumnNameManager columnNameManager, SqlConfigTypePathParser sqlPathParser, InternalSourceConfig internalSourceConfig) {
        this.columnNameManager = columnNameManager;
        this.sqlPathParser = sqlPathParser;
        this.internalSourceConfig = internalSourceConfig;
    }

    public SqlInternalFieldConfig getSqlInternalFieldConfig() {
        if (selectAllFromInputColumns()) {
            return new SqlInternalAutoFieldImport(columnNameManager);
        } else return new SqlInternalFieldImport(columnNameManager, sqlPathParser, internalSourceConfig);
    }

    private boolean selectAllFromInputColumns() {
        return SQL_PATH_SELECT_ALL_CONFIG_VALUE.equals(internalSourceConfig.getOutputField());
    }
}
