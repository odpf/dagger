package com.gojek.daggers.postProcessors.longbow.request;

import com.gojek.daggers.postProcessors.longbow.LongbowSchema;
import com.gojek.daggers.postProcessors.longbow.storage.PutRequest;
import com.gojek.daggers.sink.ProtoSerializer;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class PutRequestFactoryTest {

    @Mock
    private LongbowSchema longbowSchema;

    @Mock
    private ProtoSerializer protoSerializer;

    @Mock
    private Row row;

    private String tableId;

    @Before
    public void setup() {
        initMocks(this);
        tableId = "test_tableId";
    }

    @Test
    public void shouldCreateTablePutRequestWhenLongbowTypeIsNotLongbowPlus() {
        when(longbowSchema.isLongbowPlus()).thenReturn(false);
        PutRequestFactory putRequestFactory = new PutRequestFactory(longbowSchema, protoSerializer, tableId);
        PutRequest putRequest = putRequestFactory.create(row);
        Assert.assertEquals(putRequest.getClass(), TablePutRequest.class);
    }

    @Test
    public void shouldCreateProtoPutRequestWhenLongbowTypeIsLongbowPlus() {
        when(longbowSchema.isLongbowPlus()).thenReturn(true);
        PutRequestFactory putRequestFactory = new PutRequestFactory(longbowSchema, protoSerializer, tableId);
        PutRequest putRequest = putRequestFactory.create(row);
        Assert.assertEquals(ProtoBytePutRequest.class, putRequest.getClass());
    }
}
