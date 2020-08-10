package com.gojek.daggers.protoHandler.typeHandler;

import com.gojek.esb.types.GoFoodShoppingItemProto;
import com.google.protobuf.Descriptors;
import org.apache.flink.api.common.typeinfo.Types;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.*;

public class LongPrimitiveTypeHandlerTest {

    @Test
    public void shouldHandleLongTypes() {
        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("id");
        LongPrimitiveTypeHandler longPrimitiveTypeHandler = new LongPrimitiveTypeHandler(fieldDescriptor);
        assertTrue(longPrimitiveTypeHandler.canHandle());
    }

    @Test
    public void shouldNotHandleTypesOtherThanLong() {
        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("quantity");
        LongPrimitiveTypeHandler longPrimitiveTypeHandler = new LongPrimitiveTypeHandler(fieldDescriptor);
        assertFalse(longPrimitiveTypeHandler.canHandle());
    }

    @Test
    public void shouldFetchValueForFieldForFieldDescriptorOfTypeLong() {
        long actualValue = 2L;

        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("id");
        LongPrimitiveTypeHandler longPrimitiveTypeHandler = new LongPrimitiveTypeHandler(fieldDescriptor);
        Object value = longPrimitiveTypeHandler.getValue(actualValue);

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchParsedValueForFieldForFieldDescriptorOfTypeLong() {
        long actualValue = 2L;

        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("id");
        LongPrimitiveTypeHandler longPrimitiveTypeHandler = new LongPrimitiveTypeHandler(fieldDescriptor);
        Object value = longPrimitiveTypeHandler.getValue(String.valueOf(actualValue));

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchDefaultValueIfValueNotPresentForFieldDescriptorOfTypeLong() {
        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("id");
        LongPrimitiveTypeHandler longPrimitiveTypeHandler = new LongPrimitiveTypeHandler(fieldDescriptor);
        Object value = longPrimitiveTypeHandler.getValue(null);

        assertEquals(0L, value);
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("id");
        LongPrimitiveTypeHandler longPrimitiveTypeHandler = new LongPrimitiveTypeHandler(fieldDescriptor);
        assertEquals(Types.LONG, longPrimitiveTypeHandler.getTypeInformation());
    }

    @Test
    public void shouldReturnArrayTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("id");
        LongPrimitiveTypeHandler longPrimitiveTypeHandler = new LongPrimitiveTypeHandler(fieldDescriptor);
        assertEquals(Types.PRIMITIVE_ARRAY(Types.LONG), longPrimitiveTypeHandler.getArrayType());
    }

    @Test
    public void shouldReturnArrayValues() {
        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("id");
        LongPrimitiveTypeHandler longPrimitiveTypeHandler = new LongPrimitiveTypeHandler(fieldDescriptor);
        ArrayList<Long> inputValues = new ArrayList<>(Arrays.asList(1L, 2L, 3L));
        Object actualValues = longPrimitiveTypeHandler.getArray(inputValues);
        assertArrayEquals(inputValues.toArray(), (Long[]) actualValues);
    }

    @Test
    public void shouldReturnEmptyArrayOnNull() {
        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("id");
        LongPrimitiveTypeHandler longPrimitiveTypeHandler = new LongPrimitiveTypeHandler(fieldDescriptor);
        Object actualValues = longPrimitiveTypeHandler.getArray(null);
        assertEquals(0, ((Long[]) actualValues).length);
    }

}