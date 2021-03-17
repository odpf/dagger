package com.gojek.daggers.processors.longbow.request;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.MockitoAnnotations.initMocks;

public class ProtoByteScanRequestTest {

    private byte[] startRow;
    private byte[] endRow;
    private String tableId = "tableId";

    @Before
    public void setup() {
        initMocks(this);
        startRow = Bytes.toBytes("startRow");
        endRow = Bytes.toBytes("endRow");
    }

    @Test
    public void shouldCreateProtoScanRequest() {
        ProtoByteScanRequest protoByteScanRequest = new ProtoByteScanRequest(startRow, endRow, tableId);
        Scan expectedScan = new Scan();
        expectedScan.withStartRow(startRow, true);
        expectedScan.withStopRow(endRow, true);
        expectedScan.addColumn(Bytes.toBytes("ts"), Bytes.toBytes("proto"));
        Assert.assertTrue(expectedScan.getFamilyMap().equals(protoByteScanRequest.get().getFamilyMap()));
    }
}
