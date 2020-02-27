package com.gojek.daggers.postProcessors.longbow.data;

import com.gojek.daggers.postProcessors.longbow.LongbowSchema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class LongbowDataFactoryTest {

    @Mock
    private LongbowSchema longbowSchema;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldReturnLongbowTableDataWhenTypeIsNotLongbowPlus() {
        when(longbowSchema.isLongbowPlus()).thenReturn(false);
        LongbowDataFactory longbowDataFactory = new LongbowDataFactory(longbowSchema);
        LongbowData longbowData = longbowDataFactory.getLongbowData();
        Assert.assertEquals(LongbowTableData.class, longbowData.getClass());
    }

    @Test
    public void shouldReturnLongbowProtoDataWhenTypeIsLongbowPlus() {
        when(longbowSchema.isLongbowPlus()).thenReturn(true);
        LongbowDataFactory longbowDataFactory = new LongbowDataFactory(longbowSchema);
        LongbowData longbowData = longbowDataFactory.getLongbowData();
        Assert.assertEquals(LongbowProtoData.class, longbowData.getClass());
    }
}
