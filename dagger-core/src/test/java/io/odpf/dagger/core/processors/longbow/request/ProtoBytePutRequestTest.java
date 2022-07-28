package io.odpf.dagger.core.processors.longbow.request;

import io.odpf.dagger.common.serde.proto.serialization.ProtoSerializer;
import org.apache.flink.types.Row;

import io.odpf.dagger.core.processors.longbow.LongbowSchema;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.sql.Timestamp;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ProtoBytePutRequestTest {
    @Mock
    private ProtoSerializer protoSerializer;

    private Row input;
    private LongbowSchema longbowSchema;
    private Timestamp longbowRowtime;
    private String longbowKey = "rule123#driver444";
    private String orderNumber = "RB-9876";
    private String[] columnNames = {"longbow_key", "order_number", "rowtime"};
    private String tableId = "tableId";

    @Before
    public void setup() {
        initMocks(this);
        longbowRowtime = new Timestamp(1558498933);

        input = new Row(4);
        input.setField(0, longbowKey);
        input.setField(1, orderNumber);
        input.setField(2, longbowRowtime);

        when(protoSerializer.serializeValue(input)).thenReturn(Bytes.toBytes(" "));

        longbowSchema = new LongbowSchema(columnNames);

    }

    @Test
    public void shouldSetColumnFamilyTotsQualifierToProtoAndValueAsProtoByte() {
        ProtoBytePutRequest protoBytePutRequest = new ProtoBytePutRequest(longbowSchema, input, protoSerializer, tableId);
        Put expectedPut = new Put(longbowSchema.getKey(input, 0));
        expectedPut.addColumn(Bytes.toBytes("ts"), Bytes.toBytes("proto"), longbowRowtime.getTime(),
                protoSerializer.serializeValue(input));
        Put putRequest = protoBytePutRequest.get();

        assertEquals(expectedPut.get(Bytes.toBytes("ts"), Bytes.toBytes("proto")),
                putRequest.get(Bytes.toBytes("ts"), Bytes.toBytes("proto")));
    }
}
