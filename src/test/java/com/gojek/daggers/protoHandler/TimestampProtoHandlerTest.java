package com.gojek.daggers.protoHandler;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import com.gojek.esb.booking.BookingLogMessage;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TimestampProtoHandlerTest {
    @Test
    public void shouldReturnTrueIfTimestampFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor timestampFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("event_timestamp");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(timestampFieldDescriptor);

        assertTrue(timestampProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfFieldDescriptorOtherThanTimestampTypeIsPassed() {
        Descriptors.FieldDescriptor otherFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("order_number");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(otherFieldDescriptor);

        assertFalse(timestampProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnSameBuilderWithoutSettingFieldIfCannotHandle() {
        Descriptors.FieldDescriptor otherFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("order_number");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(otherFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(otherFieldDescriptor.getContainingType());

        DynamicMessage.Builder returnedBuilder = timestampProtoHandler.transformForKafka(builder, "123");
        assertEquals("", returnedBuilder.getField(otherFieldDescriptor));
    }

    @Test
    public void shouldReturnSameBuilderWithoutSettingFieldIfNullFieldIsPassed() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor timestampFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("event_timestamp");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(timestampFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(timestampFieldDescriptor.getContainingType());

        DynamicMessage dynamicMessage = timestampProtoHandler.transformForKafka(builder, null).build();

        BookingLogMessage bookingLogMessage = BookingLogMessage.parseFrom(dynamicMessage.toByteArray());
        assertEquals(0L, bookingLogMessage.getEventTimestamp().getSeconds());
        assertEquals(0, bookingLogMessage.getEventTimestamp().getNanos());
    }

    @Test
    public void shouldSetTimestampIfInstanceOfJavaSqlTimestampPassed() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor timestampFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("event_timestamp");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(timestampFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(timestampFieldDescriptor.getContainingType());

        long milliSeconds = System.currentTimeMillis();

        Timestamp inputTimestamp = new Timestamp(milliSeconds);
        DynamicMessage dynamicMessage = timestampProtoHandler.transformForKafka(builder, inputTimestamp).build();

        BookingLogMessage bookingLogMessage = BookingLogMessage.parseFrom(dynamicMessage.toByteArray());
        assertEquals(milliSeconds / 1000, bookingLogMessage.getEventTimestamp().getSeconds());
        assertEquals(inputTimestamp.getNanos(), bookingLogMessage.getEventTimestamp().getNanos());
    }

    @Test
    public void shouldSetTimestampIfRowHavingTimestampIsPassed() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor timestampFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("event_timestamp");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(timestampFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(timestampFieldDescriptor.getContainingType());

        long seconds = System.currentTimeMillis() / 1000;
        int nanos = (int) (System.currentTimeMillis() * 1000000);

        Row inputRow = new Row(2);
        inputRow.setField(0, seconds);
        inputRow.setField(1, nanos);

        DynamicMessage dynamicMessage = timestampProtoHandler.transformForKafka(builder, inputRow).build();

        BookingLogMessage bookingLogMessage = BookingLogMessage.parseFrom(dynamicMessage.toByteArray());
        assertEquals(seconds, bookingLogMessage.getEventTimestamp().getSeconds());
        assertEquals(nanos, bookingLogMessage.getEventTimestamp().getNanos());
    }

    @Test
    public void shouldThrowExceptionIfRowOfArityOtherThanTwoIsPassed() {
        Descriptors.FieldDescriptor timestampFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("event_timestamp");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(timestampFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(timestampFieldDescriptor.getContainingType());

        Row inputRow = new Row(3);

        try {
            timestampProtoHandler.transformForKafka(builder, inputRow).build();
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("Row: null,null,null of size: 3 cannot be converted to timestamp", e.getMessage());
        }
    }

    @Test
    public void shouldSetTimestampIfInstanceOfNumberPassed() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor timestampFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("event_timestamp");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(timestampFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(timestampFieldDescriptor.getContainingType());

        long seconds = System.currentTimeMillis() / 1000;

        DynamicMessage dynamicMessage = timestampProtoHandler.transformForKafka(builder, seconds).build();

        BookingLogMessage bookingLogMessage = BookingLogMessage.parseFrom(dynamicMessage.toByteArray());
        assertEquals(seconds, bookingLogMessage.getEventTimestamp().getSeconds());
        assertEquals(0, bookingLogMessage.getEventTimestamp().getNanos());
    }

    @Test
    public void shouldSetTimestampIfInstanceOfStringPassed() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor timestampFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("event_timestamp");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(timestampFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(timestampFieldDescriptor.getContainingType());

        String inputTimestamp = "2019-03-28T05:50:13Z";

        DynamicMessage dynamicMessage = timestampProtoHandler.transformForKafka(builder, inputTimestamp).build();

        BookingLogMessage bookingLogMessage = BookingLogMessage.parseFrom(dynamicMessage.toByteArray());
        assertEquals(1553752213, bookingLogMessage.getEventTimestamp().getSeconds());
        assertEquals(0, bookingLogMessage.getEventTimestamp().getNanos());
    }

    @Test
    public void shouldFetchTimeStampAsStringFromFieldForFieldDescriptorOfTypeTimeStampForTransformForPostProcessor() {
        String actualValue = "2018-08-30T02:21:39.975107Z";

        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("booking_creation_time");

        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(fieldDescriptor);

        Object value = protoHandler.transformFromPostProcessor(actualValue);
        assertEquals(actualValue, value);
    }

    @Test
    public void shouldReturnNullWhenTimeStampNotAvailableAndFieldDescriptorOfTypeTimeStampForTransformForPostProcessor() {
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("booking_creation_time");

        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(fieldDescriptor);

        Object value = protoHandler.transformFromPostProcessor(null);
        assertNull(value);
    }

    @Test
    public void shouldHandleTimestampMessagesByReturningNullForNonParseableTimeStampsForTransformForPostProcessor() {
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("event_timestamp");

        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(fieldDescriptor);

        Object value = protoHandler.transformFromPostProcessor("2");

        assertNull(value);
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("event_timestamp");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(fieldDescriptor);
        TypeInformation actualTypeInformation = timestampProtoHandler.getTypeInformation();
        TypeInformation<Row> expectedTypeInformation = Types.ROW_NAMED(new String[]{"seconds", "nanos"}, Types.LONG, Types.INT);
        assertEquals(expectedTypeInformation, actualTypeInformation);
    }

    @Test
    public void shouldTransformTimestampForDynamicMessageForKafka() throws InvalidProtocolBufferException {
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("event_timestamp");
        BookingLogMessage bookingLogMessage = BookingLogMessage
                .newBuilder()
                .setEventTimestamp(com.google.protobuf.Timestamp.newBuilder().setSeconds(10L).setNanos(10).build())
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(BookingLogMessage.getDescriptor(), bookingLogMessage.toByteArray());
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(fieldDescriptor);
        Row row = (Row) timestampProtoHandler.transformFromKafka(dynamicMessage.getField(fieldDescriptor));
        assertEquals(2, row.getArity());
        assertEquals(10L, row.getField(0));
        assertEquals(10, row.getField(1));
    }

    @Test
    public void shouldSetDefaultValueForDynamicMessageForKafkaIfValuesNotSet() throws InvalidProtocolBufferException {
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("event_timestamp");
        BookingLogMessage bookingLogMessage = BookingLogMessage
                .newBuilder()
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(BookingLogMessage.getDescriptor(), bookingLogMessage.toByteArray());
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(fieldDescriptor);
        Row row = (Row) timestampProtoHandler.transformFromKafka(dynamicMessage.getField(fieldDescriptor));
        assertEquals(2, row.getArity());
        assertEquals(0L, row.getField(0));
        assertEquals(0, row.getField(1));
    }

    @Test
    public void shouldConvertTimestampToJsonString() {
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("event_timestamp");

        Row inputRow = new Row(2);
        inputRow.setField(0, 1600083828L);

        Object value = new TimestampProtoHandler(fieldDescriptor).transformToJson(inputRow);

        Assert.assertEquals("2020-09-14 11:43:48", String.valueOf(value));
    }
}
