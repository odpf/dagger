package io.odpf.dagger.core.protohandler.typehandler;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import org.apache.flink.api.common.typeinfo.Types;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.*;

public class BooleanPrimitiveTypeHandlerTest {

    @Test
    public void shouldHandleBooleanTypes() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");
        BooleanPrimitiveTypeHandler booleanPrimitiveTypeHandler = new BooleanPrimitiveTypeHandler(fieldDescriptor);
        assertTrue(booleanPrimitiveTypeHandler.canHandle());
    }

    @Test
    public void shouldNotHandleTypesOtherThanBoolean() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        BooleanPrimitiveTypeHandler booleanPrimitiveTypeHandler = new BooleanPrimitiveTypeHandler(fieldDescriptor);
        assertFalse(booleanPrimitiveTypeHandler.canHandle());
    }

    @Test
    public void shouldFetchValueForFieldForFieldDescriptorOfTypeBool() {
        boolean actualValue = true;

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");
        BooleanPrimitiveTypeHandler booleanPrimitiveTypeHandler = new BooleanPrimitiveTypeHandler(fieldDescriptor);
        Object value = booleanPrimitiveTypeHandler.getValue(actualValue);

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchParsedValueForFieldForFieldDescriptorOfTypeBool() {
        boolean actualValue = true;

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");

        BooleanPrimitiveTypeHandler booleanPrimitiveTypeHandler = new BooleanPrimitiveTypeHandler(fieldDescriptor);
        Object value = booleanPrimitiveTypeHandler.getValue(String.valueOf(actualValue));

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchDefaultValueIfValueNotPresentForFieldDescriptorOfTypeBool() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");

        BooleanPrimitiveTypeHandler booleanPrimitiveTypeHandler = new BooleanPrimitiveTypeHandler(fieldDescriptor);
        Object value = booleanPrimitiveTypeHandler.getValue(null);

        assertEquals(false, value);
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");

        BooleanPrimitiveTypeHandler booleanPrimitiveTypeHandler = new BooleanPrimitiveTypeHandler(fieldDescriptor);
        assertEquals(Types.BOOLEAN, booleanPrimitiveTypeHandler.getTypeInformation());
    }

    @Test
    public void shouldReturnArrayTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");
        BooleanPrimitiveTypeHandler booleanPrimitiveTypeHandler = new BooleanPrimitiveTypeHandler(fieldDescriptor);
        assertEquals(Types.PRIMITIVE_ARRAY(Types.BOOLEAN), booleanPrimitiveTypeHandler.getArrayType());
    }

    @Test
    public void shouldReturnArrayValues() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");

        BooleanPrimitiveTypeHandler booleanPrimitiveTypeHandler = new BooleanPrimitiveTypeHandler(fieldDescriptor);
        ArrayList<Boolean> inputValues = new ArrayList<>(Arrays.asList(true, false, false));
        Object actualValues = booleanPrimitiveTypeHandler.getArray(inputValues);
        assertArrayEquals(inputValues.toArray(), (Boolean[]) actualValues);
    }

    @Test
    public void shouldReturnEmptyArrayOnNull() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");

        BooleanPrimitiveTypeHandler booleanPrimitiveTypeHandler = new BooleanPrimitiveTypeHandler(fieldDescriptor);
        Object actualValues = booleanPrimitiveTypeHandler.getArray(null);
        assertEquals(0, ((Boolean[]) actualValues).length);
    }
}
