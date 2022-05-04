package io.odpf.dagger.common.serde.typehandler.primitive;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.Types.buildMessage;
import static org.junit.Assert.*;

public class DoubleTypeHandlerTest {

    @Test
    public void shouldHandleDoubleTypes() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("cash_amount");
        DoubleTypeHandler doubleTypeHandler = new DoubleTypeHandler(fieldDescriptor);
        assertTrue(doubleTypeHandler.canHandle());
    }

    @Test
    public void shouldNotHandleTypesOtherThanDouble() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        DoubleTypeHandler doubleTypeHandler = new DoubleTypeHandler(fieldDescriptor);
        assertFalse(doubleTypeHandler.canHandle());
    }

    @Test
    public void shouldFetchValueForFieldForFieldDescriptorOfTypeDouble() {
        double actualValue = 2.0D;

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("cash_amount");
        DoubleTypeHandler doubleTypeHandler = new DoubleTypeHandler(fieldDescriptor);
        Object value = doubleTypeHandler.parseObject(actualValue);

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchParsedValueForFieldForFieldDescriptorOfTypeDouble() {
        double actualValue = 2.0D;

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("cash_amount");
        DoubleTypeHandler doubleTypeHandler = new DoubleTypeHandler(fieldDescriptor);
        Object value = doubleTypeHandler.parseObject(String.valueOf(actualValue));

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchDefaultValueIfValueNotPresentForFieldDescriptorOfTypeDouble() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("cash_amount");
        DoubleTypeHandler doubleTypeHandler = new DoubleTypeHandler(fieldDescriptor);
        Object value = doubleTypeHandler.parseObject(null);

        assertEquals(0.0D, value);
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("cash_amount");
        DoubleTypeHandler doubleTypeHandler = new DoubleTypeHandler(fieldDescriptor);
        assertEquals(Types.DOUBLE, doubleTypeHandler.getTypeInformation());
    }

    @Test
    public void shouldReturnArrayTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("cash_amount");
        DoubleTypeHandler doubleTypeHandler = new DoubleTypeHandler(fieldDescriptor);
        assertEquals(Types.PRIMITIVE_ARRAY(Types.DOUBLE), doubleTypeHandler.getArrayType());
    }

    @Test
    public void shouldReturnArrayValues() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("cash_amount");
        DoubleTypeHandler doubleTypeHandler = new DoubleTypeHandler(fieldDescriptor);
        ArrayList<Double> inputValues = new ArrayList<>(Arrays.asList(1D, 2D, 3D));
        double[] actualValues = (double[]) doubleTypeHandler.parseRepeatedObjectField(inputValues);

        assertTrue(Arrays.equals(new double[]{1D, 2D, 3D}, actualValues));
    }

    @Test
    public void shouldReturnEmptyArrayOnNull() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("cash_amount");
        DoubleTypeHandler doubleTypeHandler = new DoubleTypeHandler(fieldDescriptor);
        Object actualValues = doubleTypeHandler.parseRepeatedObjectField(null);

        assertEquals(0, ((double[]) actualValues).length);
    }

    @Test
    public void shouldFetchParsedValueForFieldOfTypeDoubleInSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("cash_amount");
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(DOUBLE).named("cash_amount")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("cash_amount", 34.23D);

        DoubleTypeHandler doubleHandler = new DoubleTypeHandler(fieldDescriptor);
        Object actualValue = doubleHandler.parseSimpleGroup(simpleGroup);

        assertEquals(34.23D, actualValue);
    }

    @Test
    public void shouldFetchDefaultValueIfFieldNotPresentInSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("cash_amount");

        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(DOUBLE).named("some-other-field")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        DoubleTypeHandler doubleHandler = new DoubleTypeHandler(fieldDescriptor);

        Object actualValue = doubleHandler.parseSimpleGroup(simpleGroup);

        assertEquals(0.0D, actualValue);
    }

    @Test
    public void shouldFetchDefaultValueIfFieldNotInitializedWithAValueInSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("cash_amount");

        /* The field is added to the schema but not assigned a value */
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(DOUBLE).named("cash_amount")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        DoubleTypeHandler doubleHandler = new DoubleTypeHandler(fieldDescriptor);

        Object actualValue = doubleHandler.parseSimpleGroup(simpleGroup);

        assertEquals(0.0D, actualValue);
    }

    @Test
    public void shouldReturnArrayOfDoubleValuesForFieldOfTypeRepeatedDoubleInsideSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("double_array_field");

        GroupType parquetSchema = buildMessage()
                .repeated(DOUBLE).named("double_array_field")
                .named("TestBookingLogMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        simpleGroup.add("double_array_field", 0.45123D);
        simpleGroup.add("double_array_field", 23.0123D);

        DoubleTypeHandler doubleHandler = new DoubleTypeHandler(fieldDescriptor);
        double[] actualValue = (double[]) doubleHandler.parseRepeatedSimpleGroupField(simpleGroup);

        assertArrayEquals(new double[]{0.45123D, 23.0123D}, actualValue, 0D);
    }

    @Test
    public void shouldReturnEmptyDoubleArrayWhenParseRepeatedSimpleGroupFieldIsCalledWithNull() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("double_array_field");

        DoubleTypeHandler doubleHandler = new DoubleTypeHandler(fieldDescriptor);
        double[] actualValue = (double[]) doubleHandler.parseRepeatedSimpleGroupField(null);

        assertArrayEquals(new double[0], actualValue, 0D);
    }

    @Test
    public void shouldReturnEmptyDoubleArrayWhenRepeatedDoubleFieldInsideSimpleGroupIsNotPresent() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("double_array_field");

        GroupType parquetSchema = buildMessage()
                .repeated(BOOLEAN).named("some_other_field")
                .named("TestBookingLogMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        DoubleTypeHandler doubleHandler = new DoubleTypeHandler(fieldDescriptor);
        double[] actualValue = (double[]) doubleHandler.parseRepeatedSimpleGroupField(simpleGroup);

        assertArrayEquals(new double[0], actualValue, 0D);
    }

    @Test
    public void shouldReturnEmptyDoubleArrayWhenRepeatedDoubleFieldInsideSimpleGroupIsNotInitialized() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("double_array_field");

        GroupType parquetSchema = buildMessage()
                .repeated(DOUBLE).named("double_array_field")
                .named("TestBookingLogMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        DoubleTypeHandler doubleHandler = new DoubleTypeHandler(fieldDescriptor);
        double[] actualValue = (double[]) doubleHandler.parseRepeatedSimpleGroupField(simpleGroup);

        assertArrayEquals(new double[0], actualValue, 0D);
    }
}