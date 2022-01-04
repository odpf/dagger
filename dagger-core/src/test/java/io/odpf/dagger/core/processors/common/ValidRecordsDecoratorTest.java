package io.odpf.dagger.core.processors.common;

import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.processors.types.FilterDecorator;
import io.odpf.dagger.common.serde.proto.deserialization.ProtoDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.List;
import java.util.stream.Collectors;

import static io.odpf.dagger.common.core.Constants.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.powermock.api.mockito.PowerMockito.when;

public class ValidRecordsDecoratorTest {

    @Mock
    private ErrorReporter errorReporter;

    @Mock
    private Configuration configuration;

    private StencilClientOrchestrator stencilClientOrchestrator;

    @Before
    public void setUp() {
        initMocks(this);
        when(configuration.getString(SCHEMA_REGISTRY_STENCIL_REFRESH_CACHE_KEY, SCHEMA_REGISTRY_STENCIL_REFRESH_CACHE_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_REFRESH_CACHE_DEFAULT);
        when(configuration.getBoolean(SCHEMA_REGISTRY_STENCIL_ENABLE_KEY, SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT);
        when(configuration.getString(SCHEMA_REGISTRY_STENCIL_URLS_KEY, SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
    }

    private String[] getColumns() {
        List<String> fields = TestBookingLogMessage.getDescriptor()
                .getFields()
                .stream()
                .map(Descriptors.FieldDescriptor::getName).collect(Collectors.toList());
        fields.add(INTERNAL_VALIDATION_FIELD_KEY);
        fields.add("rowtime");
        return fields.toArray(new String[0]);
    }

    @Test
    public void shouldThrowExceptionWithBadRecord() throws Exception {
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(TestBookingLogMessage.class.getName(), 5, "rowtime", stencilClientOrchestrator);
        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>("test-topic", 0, 0, null, "test".getBytes());
        Row invalidRow = protoDeserializer.deserialize(consumerRecord);
        ValidRecordsDecorator filter = new ValidRecordsDecorator("test", getColumns(), configuration);
        filter.errorReporter = this.errorReporter;
        InvalidProtocolBufferException exception = assertThrows(InvalidProtocolBufferException.class, () -> filter.filter(invalidRow));
        assertEquals("Bad Record Encountered for table `test`", exception.getMessage());
    }

    @Test
    public void shouldReturnTrueForCorrectRecord() throws Exception {
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(TestBookingLogMessage.class.getName(), 5, "rowtime", stencilClientOrchestrator);
        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>("test-topic", 0, 0, null, TestBookingLogMessage.newBuilder().build().toByteArray());
        Row validRow = protoDeserializer.deserialize(consumerRecord);
        FilterDecorator filter = new ValidRecordsDecorator("test", getColumns(), configuration);
        assertTrue(filter.filter(validRow));
    }
}
