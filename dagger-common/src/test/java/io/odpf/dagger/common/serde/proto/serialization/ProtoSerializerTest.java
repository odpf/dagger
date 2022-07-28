package io.odpf.dagger.common.serde.proto.serialization;

import io.odpf.dagger.common.exceptions.serde.DaggerSerializationException;
import io.odpf.dagger.common.exceptions.serde.InvalidDataTypeException;
import org.apache.flink.types.Row;

import io.odpf.stencil.StencilClientFactory;
import io.odpf.stencil.client.StencilClient;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.exceptions.DescriptorNotFoundException;
import io.odpf.dagger.common.exceptions.serde.InvalidColumnMappingException;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.consumer.TestEnrichedBookingLogMessage;
import io.odpf.dagger.consumer.TestProfile;
import io.odpf.dagger.consumer.TestSerDeLogKey;
import io.odpf.dagger.consumer.TestSerDeLogMessage;
import io.odpf.dagger.consumer.TestServiceType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ProtoSerializerTest {

    @Mock
    private StencilClientOrchestrator stencilClientOrchestrator;

    @Before
    public void setup() {
        initMocks(this);
        StencilClient stencilClient = StencilClientFactory.getClient();
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
    }

    @Test
    public void shouldSerializeKeyForProto() throws InvalidProtocolBufferException {
        String[] columnNames = {"window_start_time", "window_end_time", "s2_id_level", "s2_id", "service_type"};
        String outputProtoKey = "io.odpf.dagger.consumer.TestSerDeLogKey";
        String outputProtoMessage = "io.odpf.dagger.consumer.TestSerDeLogMessage";
        ProtoSerializer serializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator);

        long seconds = System.currentTimeMillis() / 1000;

        Row element = new Row(5);
        Timestamp timestamp = new Timestamp(seconds * 1000);
        com.google.protobuf.Timestamp expectedTimestamp = com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(seconds)
                .setNanos(0)
                .build();

        element.setField(0, timestamp);
        element.setField(1, timestamp);
        element.setField(2, 13);
        element.setField(3, 3322909458387959808L);
        element.setField(4, TestServiceType.Enum.GO_RIDE);

        byte[] keyBytes = serializer.serializeKey(element);

        TestSerDeLogKey actualKey = TestSerDeLogKey.parseFrom(keyBytes);

        assertEquals(expectedTimestamp, actualKey.getWindowStartTime());
        assertEquals(expectedTimestamp, actualKey.getWindowEndTime());
        assertEquals(13, actualKey.getS2IdLevel());
        assertEquals(3322909458387959808L, actualKey.getS2Id());
        assertEquals("GO_RIDE", actualKey.getServiceType().toString());

    }

    @Test
    public void shouldSerializeMessageProto() throws InvalidProtocolBufferException {
        String[] columnNames = {"window_start_time", "window_end_time", "s2_id_level", "s2_id", "service_type", "unique_customers",
                "event_timestamp", "string_type", "bool_type", "message_type", "repeated_message_type", "map_type"};
        String outputProtoKey = "io.odpf.dagger.consumer.TestSerDeLogKey";
        String outputProtoMessage = "io.odpf.dagger.consumer.TestSerDeLogMessage";
        ProtoSerializer serializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator);
        long seconds = System.currentTimeMillis() / 1000;

        Row element = new Row(12);
        Timestamp timestamp = new Timestamp(seconds * 1000);
        com.google.protobuf.Timestamp expectedTimestamp = com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(seconds)
                .setNanos(0)
                .build();

        element.setField(0, timestamp);
        element.setField(1, timestamp);
        element.setField(2, 13);
        element.setField(3, 3322909458387959808L);
        element.setField(4, TestServiceType.Enum.GO_RIDE);
        element.setField(5, 2L);
        element.setField(6, timestamp);
        element.setField(7, "test");
        element.setField(8, true);
        Row driverRow = new Row(1);
        driverRow.setField(0, "driver_test");
        element.setField(9, driverRow);
        Row driverIdRow = new Row(1);
        driverIdRow.setField(0, "driver_id");

        element.setField(10, new ArrayList<Row>() {{
            add(driverIdRow);
        }});
        element.setField(11, new HashMap<String, String>() {{
            put("key", "value");
        }});

        byte[] valueBytes = serializer.serializeValue(element);

        TestSerDeLogMessage actualMessage = TestSerDeLogMessage.parseFrom(valueBytes);

        assertEquals(expectedTimestamp, actualMessage.getWindowStartTime());
        assertEquals(expectedTimestamp, actualMessage.getWindowEndTime());
        assertEquals(13, actualMessage.getS2IdLevel());
        assertEquals(3322909458387959808L, actualMessage.getS2Id());
        assertEquals("GO_RIDE", actualMessage.getServiceType().toString());
        assertEquals(expectedTimestamp, actualMessage.getEventTimestamp());
        assertEquals("test", actualMessage.getStringType());
        assertTrue(actualMessage.getBoolType());
        assertEquals(TestProfile.newBuilder().setDriverId("driver_test").build(), actualMessage.getMessageType());
        assertEquals(new ArrayList<TestProfile>() {{
            add(TestProfile.newBuilder().setDriverId("driver_id").build());
        }}, actualMessage.getRepeatedMessageTypeList());
        assertEquals(new HashMap<String, String>() {{
            put("key", "value");
        }}, actualMessage.getMapTypeMap());
    }


    @Test
    public void shouldSerializeDataForOneFieldInNestedProtoWhenMappedFromQuery() throws InvalidProtocolBufferException {
        String[] columnNames = {"customer_profile.customer_id"};
        String outputProtoKey = "io.odpf.dagger.consumer.TestEnrichedBookingLogMessage";
        String outputProtoMessage = "io.odpf.dagger.consumer.TestEnrichedBookingLogMessage";
        ProtoSerializer serializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator);

        Row element = new Row(1);

        element.setField(0, "test-id");

        byte[] value = serializer.serializeValue(element);

        TestEnrichedBookingLogMessage actualValue = TestEnrichedBookingLogMessage.parseFrom(value);

        assertEquals("test-id", actualValue.getCustomerProfile().getCustomerId());
    }

    @Test
    public void shouldSerializeDataForMultipleFieldsInSameNestedProtoWhenMappedFromQuery() throws InvalidProtocolBufferException {
        String[] columnNames = {"customer_profile.name", "customer_profile.email", "customer_profile.phone_verified"};
        String outputProtoKey = "io.odpf.dagger.consumer.TestEnrichedBookingLogMessage";
        String outputProtoMessage = "io.odpf.dagger.consumer.TestEnrichedBookingLogMessage";
        ProtoSerializer serializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator);

        Row element = new Row(3);

        element.setField(0, "test-name");
        element.setField(1, "test_email@go-jek.com");
        element.setField(2, true);

        byte[] valueBytes = serializer.serializeValue(element);

        TestEnrichedBookingLogMessage actualValue = TestEnrichedBookingLogMessage.parseFrom(valueBytes);

        assertEquals("test-name", actualValue.getCustomerProfile().getName());
        assertEquals("test_email@go-jek.com", actualValue.getCustomerProfile().getEmail());
        assertTrue(actualValue.getCustomerProfile().getPhoneVerified());
    }

    @Test
    public void shouldSerializeDataForMultipleFieldsInDifferentNestedProtoWhenMappedFromQuery() throws InvalidProtocolBufferException {
        String[] columnNames = {"order_number", "service_type", "customer_price", "customer_total_fare_without_surge", "driver_pickup_location.name", "driver_pickup_location.latitude"};
        String outputProtoKey = "io.odpf.dagger.consumer.TestBookingLogKey";
        String outputProtoMessage = "io.odpf.dagger.consumer.TestBookingLogMessage";
        ProtoSerializer serializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator);

        Row element = new Row(6);

        element.setField(0, "order_number");
        element.setField(1, "GO_RIDE");
        element.setField(2, 123D);
        element.setField(3, 12345L);
        element.setField(4, "driver_name");
        element.setField(5, 876D);

        byte[] valueBytes = serializer.serializeValue(element);

        TestBookingLogMessage actualValue = TestBookingLogMessage.parseFrom(valueBytes);

        assertEquals("order_number", actualValue.getOrderNumber());
        assertEquals(TestServiceType.Enum.GO_RIDE, actualValue.getServiceType());
        assertEquals(123D, actualValue.getCustomerPrice(), 0D);
        assertEquals(12345L, actualValue.getCustomerTotalFareWithoutSurge());
        assertEquals("driver_name", actualValue.getDriverPickupLocation().getName());
        assertEquals(876D, actualValue.getDriverPickupLocation().getLatitude(), 0D);
    }

    @Test
    public void shouldThrowExceptionWhenColumnDoesNotExists() {
        String[] columnNames = {"order_number", "driver_pickup_location.invalid"};
        String outputProtoKey = "io.odpf.dagger.consumer.TestBookingLogKey";
        String outputProtoMessage = "io.odpf.dagger.consumer.TestBookingLogMessage";
        ProtoSerializer serializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator);
        Row element = new Row(2);
        element.setField(0, "order_number");
        element.setField(1, 876D);

        InvalidColumnMappingException exception = assertThrows(InvalidColumnMappingException.class,
                () -> serializer.serializeValue(element));
        assertEquals("column invalid doesn't exists in the proto of io.odpf.dagger.consumer.TestLocation",
                exception.getMessage());

    }

    @Test
    public void shouldMapOtherFieldsWhenOneOfTheFirstFieldIsInvalidForANestedFieldInTheQuery() throws InvalidProtocolBufferException {
        String[] columnNames = {"blah.invalid", "customer_email"};
        String outputProtoKey = "io.odpf.dagger.consumer.TestBookingLogKey";
        String outputProtoMessage = "io.odpf.dagger.consumer.TestBookingLogMessage";
        ProtoSerializer serializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator);
        Row element = new Row(2);
        element.setField(0, "order_number");
        element.setField(1, "customer_email@go-jek.com");

        byte[] valueBytes = serializer.serializeValue(element);

        assertEquals("customer_email@go-jek.com", TestBookingLogMessage.parseFrom(valueBytes).getCustomerEmail());
    }

    @Test
    public void shouldMapEmptyDataWhenFieldIsInvalidInTheQuery() {
        String[] columnNames = {"invalid"};
        String outputProtoKey = "io.odpf.dagger.consumer.TestBookingLogKey";
        String outputProtoMessage = "io.odpf.dagger.consumer.TestBookingLogMessage";
        ProtoSerializer serializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator);
        Row element = new Row(1);
        element.setField(0, "order_number");

        byte[] valueBytes = serializer.serializeValue(element);

        assertEquals(0, valueBytes.length);
    }

    @Test
    public void shouldMapOtherFieldsWhenOneOfTheFieldIsInvalidInTheQuery() throws InvalidProtocolBufferException {
        String[] columnNames = {"invalid", "order_number"};
        String outputProtoKey = "io.odpf.dagger.consumer.TestBookingLogKey";
        String outputProtoMessage = "io.odpf.dagger.consumer.TestBookingLogMessage";
        ProtoSerializer serializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator);
        Row element = new Row(2);
        element.setField(0, "some_data");
        element.setField(1, "order_number");

        byte[] valueBytes = serializer.serializeValue(element);

        assertEquals("order_number", TestBookingLogMessage.parseFrom(valueBytes).getOrderNumber());
    }

    @Test
    public void shouldNotThrowExceptionWhenPrimitiveTypeCanBeCasted() throws InvalidProtocolBufferException {
        String[] columnNames = {"order_number"};
        String outputProtoKey = "io.odpf.dagger.consumer.TestBookingLogKey";
        String outputProtoMessage = "io.odpf.dagger.consumer.TestBookingLogMessage";
        ProtoSerializer serializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator);
        Row element = new Row(1);
        element.setField(0, 1234);

        byte[] testBookingLogMessage = serializer.serializeValue(element);
        assertEquals("1234", TestBookingLogMessage.parseFrom(testBookingLogMessage).getOrderNumber());
    }

    @Test
    public void shouldThrowExceptionWhenPrimitiveTypeCanNotBeCasted() {
        String[] columnNames = {"customer_price"};
        String outputProtoKey = "io.odpf.dagger.consumer.TestBookingLogKey";
        String outputProtoMessage = "io.odpf.dagger.consumer.TestBookingLogMessage";
        ProtoSerializer serializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator);
        Row element = new Row(1);
        element.setField(0, "invalid_number");

        InvalidDataTypeException exception = assertThrows(InvalidDataTypeException.class,
                () -> serializer.serializeValue(element));
        assertEquals("type mismatch of field: customer_price, expecting DOUBLE type, actual type class java.lang.String",
                exception.getMessage());
    }

    @Test
    public void shouldHandleRepeatedTypeWhenTypeDoesNotMatch() {

        String[] columnNames = {"meta_array"};
        String outputProtoKey = "io.odpf.dagger.consumer.TestBookingLogKey";
        String outputProtoMessage = "io.odpf.dagger.consumer.TestBookingLogMessage";
        ProtoSerializer serializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator);
        Row element = new Row(1);
        element.setField(0, 1234);

        InvalidColumnMappingException exception = assertThrows(InvalidColumnMappingException.class,
                () -> serializer.serializeValue(element));
        assertEquals("column invalid: type mismatch of column meta_array, expecting REPEATED STRING type. Actual type class java.lang.Integer",
                exception.getMessage());
    }

    @Test
    public void shouldSerializeMessageWhenOnlyMessageProtoProvided() throws InvalidProtocolBufferException {
        String[] columnNames = {"order_number", "driver_id"};
        String outputProtoMessage = "io.odpf.dagger.consumer.TestBookingLogMessage";
        ProtoSerializer serializer = new ProtoSerializer(null, outputProtoMessage, columnNames, stencilClientOrchestrator);

        String orderNumber = "RB-1234";

        Row element = new Row(2);
        element.setField(0, orderNumber);
        element.setField(1, "DR-124");

        byte[] valueBytes = serializer.serializeValue(element);
        TestBookingLogMessage actualMessage = TestBookingLogMessage.parseFrom(valueBytes);

        assertEquals(orderNumber, actualMessage.getOrderNumber());
    }

    @Test
    public void shouldReturnNullKeyWhenOnlyMessageProtoProvided() {
        String[] columnNames = {"s2_id_level"};
        String protoMessage = "io.odpf.dagger.consumer.TestSerDeLogMessage";
        ProtoSerializer serializer = new ProtoSerializer(null, protoMessage, columnNames,
                stencilClientOrchestrator);

        Row element = new Row(1);
        element.setField(0, 13);

        byte[] valueBytes = serializer.serializeValue(element);
        byte[] keyBytes = serializer.serializeKey(element);

        assertNull(keyBytes);
        assertNotNull(valueBytes);
    }

    @Test
    public void shouldReturnNullKeyWhenKeyIsEmptyString() {
        String[] columnNames = {"s2_id_level"};
        String protoMessage = "io.odpf.dagger.consumer.TestSerDeLogMessage";
        ProtoSerializer serializer = new ProtoSerializer("", protoMessage, columnNames, stencilClientOrchestrator);

        Row element = new Row(1);
        element.setField(0, 13);

        byte[] keyBytes = serializer.serializeKey(element);
        byte[] valueBytes = serializer.serializeValue(element);

        assertNull(keyBytes);
        assertNotNull(valueBytes);
    }

    @Test(expected = DescriptorNotFoundException.class)
    public void shouldThrowDescriptorNotFoundException() {
        String[] columnNames = {"s2_id_level"};
        String protoMessage = "RandomMessageClass";
        ProtoSerializer serializer = new ProtoSerializer(null, protoMessage, columnNames, stencilClientOrchestrator);

        int s2IdLevel = 13;
        Row element = new Row(1);
        element.setField(0, s2IdLevel);

        serializer.serializeValue(element);
    }

    @Test(expected = DaggerSerializationException.class)
    public void shouldThrowExceptionIfTheValueMessageClassIsMissing() {
        String[] columnNames = {"s2_id_level"};
        ProtoSerializer serializer = new ProtoSerializer("keyMessage", null, columnNames, stencilClientOrchestrator);

        Row element = new Row(1);
        element.setField(0, 13);

        serializer.serializeValue(element);
    }

    @Test(expected = DaggerSerializationException.class)
    public void shouldThrowExceptionIfTheValueMessageClassIsEmpty() {
        String[] columnNames = {"s2_id_level"};
        ProtoSerializer serializer = new ProtoSerializer("keyMessage", "", columnNames, stencilClientOrchestrator);

        Row element = new Row(1);
        element.setField(0, 13);

        serializer.serializeValue(element);
    }
}
