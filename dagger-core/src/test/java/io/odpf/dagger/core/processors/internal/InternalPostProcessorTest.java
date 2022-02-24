package io.odpf.dagger.core.processors.internal;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.core.exception.InvalidConfigurationException;
import io.odpf.dagger.core.processors.PostProcessorConfig;
import io.odpf.dagger.core.processors.external.ExternalSourceConfig;
import io.odpf.dagger.core.processors.transformers.TransformConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class InternalPostProcessorTest {
    @Mock
    private Configuration configuration;

    @Before
    public void setup() {
        initMocks(this);
        when(configuration.getString("STREAMS", ""))
                .thenReturn("[{\"INPUT_SCHEMA_PROTO_CLASS\": \"io.odpf.dagger.consumer.TestBookingLogMessage\"}]");
        when(configuration.getBoolean("SCHEMA_REGISTRY_STENCIL_ENABLE", false))
                .thenReturn(false);
        when(configuration.getString("SCHEMA_REGISTRY_STENCIL_URLS", ""))
                .thenReturn("");
    }

    @Test
    public void canProcessWhenInternalConfigIsPresent() {
        InternalPostProcessor internalPostProcessor = new InternalPostProcessor(null, null);

        PostProcessorConfig mockConfig = mock(PostProcessorConfig.class);
        when(mockConfig.hasInternalSource()).thenReturn(true);
        assertTrue(internalPostProcessor.canProcess(mockConfig));
    }

    @Test
    public void canNotProcessWhenInternalConfigIsNull() {
        InternalPostProcessor internalPostProcessor = new InternalPostProcessor(null, null);

        PostProcessorConfig mockConfig = mock(PostProcessorConfig.class);
        when(mockConfig.hasInternalSource()).thenReturn(false);
        assertFalse(internalPostProcessor.canProcess(mockConfig));
    }

    @Test
    public void shouldNotBeAbleToProcessWhenInternalConfigIsInvalid() {
        String exceptionMsg = "Missing required fields: [output_field]";
        IllegalArgumentException exception = new IllegalArgumentException(exceptionMsg);
        InternalSourceConfig mockConfig = mock(InternalSourceConfig.class);
        doThrow(exception)
                .when(mockConfig).validateFields();
        List<InternalSourceConfig> internalSource = Arrays.asList(mockConfig);
        PostProcessorConfig postProcessorConfig = new PostProcessorConfig(null, Collections.emptyList(), internalSource);
        InternalPostProcessor internalPostProcessor = new InternalPostProcessor(postProcessorConfig, configuration);
        StreamInfo streamInfoMock = mock(StreamInfo.class);
        when(streamInfoMock.getColumnNames()).thenReturn(new String[] {"order_id", "customer_id"});

        IllegalArgumentException actualException = assertThrows(IllegalArgumentException.class,
                () -> internalPostProcessor.process(streamInfoMock));
        assertEquals(exceptionMsg, actualException.getMessage());
    }

    @Test
    public void shouldNotBeAbleToProcessWhenDaggerConfigIsInvalid() {
        ExternalSourceConfig externalSource = new ExternalSourceConfig(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
        ArrayList<TransformConfig> transformers = new ArrayList<>();
        ArrayList<InternalSourceConfig> internalSourceConfigs = new ArrayList<>();
        internalSourceConfigs.add(new InternalSourceConfig("output", "order_id", "sql", null));

        PostProcessorConfig postProcessorConfig = new PostProcessorConfig(externalSource, transformers, internalSourceConfigs);

        InternalPostProcessor internalPostProcessor = new InternalPostProcessor(postProcessorConfig, null);
        StreamInfo streamInfoMock = mock(StreamInfo.class);
        when(streamInfoMock.getColumnNames()).thenReturn(new String[] {"order_id", "customer_id"});

        InvalidConfigurationException actualException = assertThrows(InvalidConfigurationException.class,
                () -> {
                    internalPostProcessor.process(streamInfoMock);
                });
        assertEquals("Invalid configuration: null", actualException.getMessage());
    }

    @Test
    public void processWithRightConfiguration() {
        ExternalSourceConfig externalSource = new ExternalSourceConfig(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
        ArrayList<TransformConfig> transformers = new ArrayList<>();
        ArrayList<InternalSourceConfig> internalSourceConfigs = new ArrayList<>();
        internalSourceConfigs.add(new InternalSourceConfig("output", "order_id", "sql", null));
        PostProcessorConfig postProcessorConfig = new PostProcessorConfig(externalSource, transformers, internalSourceConfigs);
        InternalPostProcessor internalPostProcessor = new InternalPostProcessor(postProcessorConfig, configuration);

        StreamInfo streamInfoMock = mock(StreamInfo.class);
        DataStream resultStream = mock(DataStream.class);
        when(streamInfoMock.getColumnNames()).thenReturn(new String[] {"order_id", "customer_id"});
        when(streamInfoMock.getDataStream()).thenReturn(resultStream);

        StreamInfo process = internalPostProcessor.process(streamInfoMock);
        verify(resultStream, times(1)).map(any(InternalDecorator.class));
        assertArrayEquals(new String[] {"output"}, process.getColumnNames());
    }
}
