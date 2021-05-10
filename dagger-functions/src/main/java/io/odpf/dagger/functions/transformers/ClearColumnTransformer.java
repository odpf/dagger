package io.odpf.dagger.functions.transformers;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.common.core.Transformer;

import java.util.Arrays;
import java.util.Map;

public class ClearColumnTransformer implements MapFunction<Row, Row>, Transformer {
    private static final String TARGET_KEY_COLUMN_NAME = "targetColumnName";
    private final String targetColumnName;
    private String[] columnNames;

    public ClearColumnTransformer(Map<String, String> transformationArguments, String[] columnNames, Configuration configuration) {
        this.columnNames = columnNames;
        this.targetColumnName = transformationArguments.get(TARGET_KEY_COLUMN_NAME);
    }

    @Override
    public Row map(Row inputRow) throws IllegalArgumentException {
        int targetFieldIndex = Arrays.asList(columnNames).indexOf(targetColumnName);
        if (targetFieldIndex == -1) {
            throw new IllegalArgumentException("Target Column is not defined OR doesn't exists");
        }
        Row outputRow = new Row(inputRow.getArity());
        for (int i = 0; i < inputRow.getArity(); i++) {
            outputRow.setField(i, inputRow.getField(i));
        }
        outputRow.setField(targetFieldIndex, "");
        return outputRow;
    }

    @Override
    public StreamInfo transform(StreamInfo inputStreamInfo) {
        DataStream<Row> inputStream = inputStreamInfo.getDataStream();
        SingleOutputStreamOperator<Row> outputStream = inputStream.map(this);
        return new StreamInfo(outputStream, inputStreamInfo.getColumnNames());
    }

}

