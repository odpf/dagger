package io.odpf.dagger.common.serde.proto.protohandler;

import io.odpf.dagger.common.exceptions.serde.InvalidDataTypeException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.junit.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.junit.Assert.*;

public class TimestampProtoHandlerTest {
    @Test
    public void shouldReturnTrueIfTimestampFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor timestampFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("event_timestamp");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(timestampFieldDescriptor);

        assertTrue(timestampProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfFieldDescriptorOtherThanTimestampTypeIsPassed() {
        Descriptors.FieldDescriptor otherFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(otherFieldDescriptor);

        assertFalse(timestampProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnSameBuilderWithoutSettingFieldIfCannotHandle() {
        Descriptors.FieldDescriptor otherFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(otherFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(otherFieldDescriptor.getContainingType());

        DynamicMessage.Builder returnedBuilder = timestampProtoHandler.transformForKafka(builder, "123");
        assertEquals("", returnedBuilder.getField(otherFieldDescriptor));
    }

    @Test
    public void shouldReturnSameBuilderWithoutSettingFieldIfNullFieldIsPassed() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor timestampFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("event_timestamp");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(timestampFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(timestampFieldDescriptor.getContainingType());

        DynamicMessage dynamicMessage = timestampProtoHandler.transformForKafka(builder, null).build();

        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.parseFrom(dynamicMessage.toByteArray());
        assertEquals(0L, bookingLogMessage.getEventTimestamp().getSeconds());
        assertEquals(0, bookingLogMessage.getEventTimestamp().getNanos());
    }

    @Test
    public void shouldSetTimestampIfInstanceOfJavaSqlTimestampPassed() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor timestampFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("event_timestamp");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(timestampFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(timestampFieldDescriptor.getContainingType());

        long milliSeconds = System.currentTimeMillis();

        Timestamp inputTimestamp = new Timestamp(milliSeconds);
        DynamicMessage dynamicMessage = timestampProtoHandler.transformForKafka(builder, inputTimestamp).build();

        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.parseFrom(dynamicMessage.toByteArray());
        assertEquals(milliSeconds / 1000, bookingLogMessage.getEventTimestamp().getSeconds());
        assertEquals(inputTimestamp.getNanos(), bookingLogMessage.getEventTimestamp().getNanos());
    }

    @Test
    public void shouldSetTimestampIfInstanceOfLocalDateTimePassed() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor timestampFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("event_timestamp");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(timestampFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(timestampFieldDescriptor.getContainingType());

        long milliSeconds = System.currentTimeMillis();

        Timestamp inputTimestamp = new Timestamp(milliSeconds);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(milliSeconds), ZoneOffset.UTC);

        DynamicMessage dynamicMessage = timestampProtoHandler.transformForKafka(builder, localDateTime).build();

        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.parseFrom(dynamicMessage.toByteArray());
        assertEquals(milliSeconds / 1000, bookingLogMessage.getEventTimestamp().getSeconds());
    }

    @Test
    public void shouldSetTimestampIfRowHavingTimestampIsPassed() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor timestampFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("event_timestamp");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(timestampFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(timestampFieldDescriptor.getContainingType());

        long seconds = System.currentTimeMillis() / 1000;
        int nanos = (int) (System.currentTimeMillis() * 1000000);

        Row inputRow = Row.of(seconds, nanos);
        DynamicMessage dynamicMessage = timestampProtoHandler.transformForKafka(builder, inputRow).build();

        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.parseFrom(dynamicMessage.toByteArray());
        assertEquals(seconds, bookingLogMessage.getEventTimestamp().getSeconds());
        assertEquals(nanos, bookingLogMessage.getEventTimestamp().getNanos());
    }

    @Test
    public void shouldThrowExceptionIfRowOfArityOtherThanTwoIsPassed() {
        Descriptors.FieldDescriptor timestampFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("event_timestamp");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(timestampFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(timestampFieldDescriptor.getContainingType());

        Row inputRow = new Row(3);

        try {
            timestampProtoHandler.transformForKafka(builder, inputRow).build();
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("Row: +I[null, null, null] of size: 3 cannot be converted to timestamp", e.getMessage());
        }
    }

    @Test
    public void shouldSetTimestampIfInstanceOfNumberPassed() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor timestampFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("event_timestamp");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(timestampFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(timestampFieldDescriptor.getContainingType());

        long seconds = System.currentTimeMillis() / 1000;

        DynamicMessage dynamicMessage = timestampProtoHandler.transformForKafka(builder, seconds).build();

        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.parseFrom(dynamicMessage.toByteArray());
        assertEquals(seconds, bookingLogMessage.getEventTimestamp().getSeconds());
        assertEquals(0, bookingLogMessage.getEventTimestamp().getNanos());
    }

    @Test
    public void shouldSetTimestampIfInstanceOfStringPassed() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor timestampFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("event_timestamp");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(timestampFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(timestampFieldDescriptor.getContainingType());

        String inputTimestamp = "2019-03-28T05:50:13Z";

        DynamicMessage dynamicMessage = timestampProtoHandler.transformForKafka(builder, inputTimestamp).build();

        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.parseFrom(dynamicMessage.toByteArray());
        assertEquals(1553752213, bookingLogMessage.getEventTimestamp().getSeconds());
        assertEquals(0, bookingLogMessage.getEventTimestamp().getNanos());
    }

    @Test
    public void shouldFetchTimeStampAsStringFromFieldForFieldDescriptorOfTypeTimeStampForTransformForPostProcessor() {
        String strValue = "2018-08-30T02:21:39.975107Z";

        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("booking_creation_time");

        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(fieldDescriptor);

        Object value = protoHandler.transformFromPostProcessor(strValue);
        assertEquals(strValue, value);
    }

    @Test
    public void shouldReturnNullWhenTimeStampNotAvailableAndFieldDescriptorOfTypeTimeStampForTransformForPostProcessor() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("booking_creation_time");

        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(fieldDescriptor);

        Object value = protoHandler.transformFromPostProcessor(null);
        assertNull(value);
    }

    @Test
    public void shouldHandleTimestampMessagesByReturningNullForNonParseableTimeStampsForTransformForPostProcessor() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("event_timestamp");

        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(fieldDescriptor);

        Object value = protoHandler.transformFromPostProcessor("2");

        assertNull(value);
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("event_timestamp");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(fieldDescriptor);
        TypeInformation actualTypeInformation = timestampProtoHandler.getTypeInformation();
        TypeInformation<Row> expectedTypeInformation = Types.ROW_NAMED(new String[]{"seconds", "nanos"}, Types.LONG, Types.INT);
        assertEquals(expectedTypeInformation, actualTypeInformation);
    }

    @Test
    public void shouldTransformTimestampForDynamicMessageForKafka() throws InvalidProtocolBufferException {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("event_timestamp");
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage
                .newBuilder()
                .setEventTimestamp(com.google.protobuf.Timestamp.newBuilder().setSeconds(10L).setNanos(10).build())
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), bookingLogMessage.toByteArray());
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(fieldDescriptor);
        Row row = (Row) timestampProtoHandler.transformFromKafka(dynamicMessage.getField(fieldDescriptor));
        assertEquals(Row.of(10L, 10), row);
    }

    @Test
    public void shouldSetDefaultValueForDynamicMessageForKafkaIfValuesNotSet() throws InvalidProtocolBufferException {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("event_timestamp");
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage
                .newBuilder()
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), bookingLogMessage.toByteArray());
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(fieldDescriptor);
        Row row = (Row) timestampProtoHandler.transformFromKafka(dynamicMessage.getField(fieldDescriptor));
        assertEquals(Row.of(0L, 0), row);
    }

    @Test
    public void shouldConvertTimestampToJsonString() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("event_timestamp");

        Row inputRow = new Row(2);
        inputRow.setField(0, 1600083828L);

        Object value = new TimestampProtoHandler(fieldDescriptor).transformToJson(inputRow);

        assertEquals("2020-09-14 11:43:48", String.valueOf(value));
    }

    @Test
    public void shouldTransformEpochInMillisFromSimpleGroup() {
        long sampleTimeInMillis = Instant.now().toEpochMilli();
        Instant instant = Instant.ofEpochMilli(sampleTimeInMillis);
        Row expectedRow = Row.of(instant.getEpochSecond(), instant.getNano());

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("event_timestamp");
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(INT64).named("event_timestamp")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("event_timestamp", sampleTimeInMillis);

        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(fieldDescriptor);
        Row actualRow = (Row) timestampProtoHandler.transformFromParquet(simpleGroup);

        assertEquals(expectedRow, actualRow);
    }

    @Test
    public void shouldThrowExceptionIfNullIsPassedToTransformFromParquet() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("event_timestamp");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(fieldDescriptor);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> timestampProtoHandler.transformFromParquet(null));
        assertEquals("Could not extract timestamp with descriptor name event_timestamp from null", exception.getMessage());
    }

    @Test
    public void shouldThrowExceptionIfAnyArgumentOtherThanSimpleGroupIsPassedToTransformFromParquet() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("event_timestamp");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(fieldDescriptor);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> timestampProtoHandler.transformFromParquet("some object"));
        assertEquals("Could not extract timestamp with descriptor name event_timestamp from some object", exception.getMessage());
    }

    @Test
    public void shouldThrowExceptionDuringTransformIfSimpleGroupDoesNotContainField() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("event_timestamp");
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(INT64).named("some-other-field")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(fieldDescriptor);

        InvalidDataTypeException exception = assertThrows(InvalidDataTypeException.class,
                () -> timestampProtoHandler.transformFromParquet(simpleGroup));
        String expectedErrorMessage = "Could not extract timestamp with descriptor name event_timestamp from simple " +
                "group of type: required group TestGroupType {\n" +
                "  required int64 some-other-field;\n" +
                "}";
        assertEquals(expectedErrorMessage, exception.getMessage());
    }

    @Test
    public void shouldThrowExceptionDuringTransformIfSimpleGroupDoesNotContainValueForField() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("event_timestamp");
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(INT64).named("event_timestamp")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(fieldDescriptor);

        InvalidDataTypeException exception = assertThrows(InvalidDataTypeException.class,
                () -> timestampProtoHandler.transformFromParquet(simpleGroup));
        String expectedErrorMessage = "Could not extract timestamp with descriptor name event_timestamp from " +
                "simple group of type: required group TestGroupType {\n" +
                "  required int64 event_timestamp;\n" +
                "}";
        assertEquals(expectedErrorMessage, exception.getMessage());
    }
}