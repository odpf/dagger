package io.odpf.dagger.core.processors.longbow.request;

import org.apache.flink.types.Row;

import io.odpf.dagger.core.processors.longbow.LongbowSchema;
import io.odpf.dagger.core.processors.longbow.storage.PutRequest;
import io.odpf.dagger.common.serde.proto.serialization.KafkaProtoSerializer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class PutRequestFactoryTest {

    @Mock
    private LongbowSchema longbowSchema;

    @Mock
    private KafkaProtoSerializer kafkaProtoSerializer;

    private Row row;

    private String tableId;

    @Before
    public void setup() {
        initMocks(this);
        tableId = "test_tableId";
        this.row = new Row(1);
    }

    @Test
    public void shouldCreateTablePutRequestWhenLongbowTypeIsNotLongbowPlus() {
        when(longbowSchema.isLongbowPlus()).thenReturn(false);
        PutRequestFactory putRequestFactory = new PutRequestFactory(longbowSchema, kafkaProtoSerializer, tableId);

        PutRequest putRequest = putRequestFactory.create(row);
        assertEquals(TablePutRequest.class, putRequest.getClass());
    }

    @Test
    public void shouldCreateProtoPutRequestWhenLongbowTypeIsLongbowPlus() {
        when(longbowSchema.isLongbowPlus()).thenReturn(true);
        PutRequestFactory putRequestFactory = new PutRequestFactory(longbowSchema, kafkaProtoSerializer, tableId);
        PutRequest putRequest = putRequestFactory.create(row);

        assertEquals(ProtoBytePutRequest.class, putRequest.getClass());
    }
}
