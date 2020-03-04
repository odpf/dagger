package com.gojek.daggers.postProcessors.longbow.data;

import com.gojek.daggers.postProcessors.longbow.LongbowSchema;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.gojek.daggers.utils.Constants.LONGBOW_COLUMN_FAMILY_DEFAULT;
import static com.gojek.daggers.utils.Constants.LONGBOW_DATA;

public class LongbowTableData implements LongbowData {

    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes(LONGBOW_COLUMN_FAMILY_DEFAULT);
    private LongbowSchema longbowSchema;

    public LongbowTableData(LongbowSchema longbowSchema) {
        this.longbowSchema = longbowSchema;
    }

    @Override
    public Map<String, List<String>> parse(List<Result> scanResult) {
        Map<String, List<String>> longbowData = new HashMap<>();
        if (scanResult.isEmpty()) {
            longbowSchema.getColumnNames().forEach(name -> longbowData.put(name, new ArrayList<>()));
        } else {
            longbowSchema.getColumnNames().forEach(name -> longbowData.put(name, getData(scanResult, name)));
        }
        return longbowData;
    }

    private List<String> getData(List<Result> resultScan, String name) {
        return resultScan
                .stream()
                .map(result -> Bytes.toString(result.getValue(COLUMN_FAMILY_NAME, Bytes.toBytes(name))))
                .collect(Collectors.toList());
    }
}
