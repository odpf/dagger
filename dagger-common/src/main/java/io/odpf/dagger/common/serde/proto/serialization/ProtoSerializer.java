package io.odpf.dagger.common.serde.proto.serialization;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.exceptions.DescriptorNotFoundException;
import io.odpf.dagger.common.exceptions.serde.DaggerSerializationException;
import io.odpf.dagger.common.exceptions.serde.InvalidColumnMappingException;
import org.apache.flink.types.Row;
import io.odpf.dagger.common.serde.typehandler.TypeHandler;
import io.odpf.dagger.common.serde.typehandler.TypeHandlerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public class ProtoSerializer implements Serializable {

    private final String keyProtoClassName;
    private final String[] columnNames;
    private final StencilClientOrchestrator stencilClientOrchestrator;
    private final String messageProtoClassName;

    public ProtoSerializer(String keyProtoClassName, String messageProtoClassName, String[] columnNames, StencilClientOrchestrator stencilClientOrchestrator) {
        this.keyProtoClassName = keyProtoClassName;
        this.columnNames = columnNames;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.messageProtoClassName = messageProtoClassName;
        checkValidity();
    }

    private void checkValidity() {
        if (Objects.isNull(messageProtoClassName) || messageProtoClassName.isEmpty()) {
            throw new DaggerSerializationException("messageProtoClassName is required");
        }
    }

    /**
     * Serialize key message.
     *
     * @param row the row
     * @return the byte [ ]
     */
    public byte[] serializeKey(Row row) {
        return (Objects.isNull(keyProtoClassName) || keyProtoClassName.isEmpty()) ? null
                : parse(row, getDescriptor(keyProtoClassName)).toByteArray();
    }

    public byte[] serializeValue(Row row) {
        return parse(row, getDescriptor(messageProtoClassName)).toByteArray();
    }

    private DynamicMessage parse(Row element, Descriptors.Descriptor descriptor) {
        int numberOfElements = element.getArity();
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        for (int index = 0; index < numberOfElements; index++) {
            String columnName = columnNames[index];
            Object data = element.getField(index);
            String[] nestedColumnNames = columnName.split("\\.");
            if (nestedColumnNames.length > 1) {
                Descriptors.FieldDescriptor firstField = descriptor.findFieldByName(nestedColumnNames[0]);
                if (firstField == null) {
                    continue;
                }
                builder = populateNestedBuilder(descriptor, nestedColumnNames, builder, data);
            } else {
                Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(columnName);
                builder = populateBuilder(builder, fieldDescriptor, data);
            }
        }
        return builder.build();
    }

    private Descriptors.Descriptor getDescriptor(String className) {
        Descriptors.Descriptor dsc = stencilClientOrchestrator.getStencilClient().get(className);
        if (dsc == null) {
            throw new DescriptorNotFoundException();
        }
        return dsc;
    }

    private DynamicMessage.Builder populateNestedBuilder(Descriptors.Descriptor parentDescriptor, String[] nestedColumnNames, DynamicMessage.Builder parentBuilder, Object data) {
        String childColumnName = nestedColumnNames[0];
        Descriptors.FieldDescriptor childFieldDescriptor = parentDescriptor.findFieldByName(childColumnName);
        if (childFieldDescriptor == null) {
            throw new InvalidColumnMappingException(String.format("column %s doesn't exists in the proto of %s", childColumnName, parentDescriptor.getFullName()));
        }
        if (nestedColumnNames.length == 1) {
            return populateBuilder(parentBuilder, childFieldDescriptor, data);
        }
        Descriptors.Descriptor childDescriptor = childFieldDescriptor.getMessageType();
        DynamicMessage.Builder childBuilder = DynamicMessage.newBuilder(childDescriptor);
        childBuilder.mergeFrom((DynamicMessage) parentBuilder.build().getField(childFieldDescriptor));
        parentBuilder.setField(childFieldDescriptor, populateNestedBuilder(childDescriptor, Arrays.copyOfRange(nestedColumnNames, 1, nestedColumnNames.length), childBuilder, data).build());
        return parentBuilder;
    }

    private DynamicMessage.Builder populateBuilder(DynamicMessage.Builder builder, Descriptors.FieldDescriptor fieldDescriptor, Object data) {
        if (fieldDescriptor == null) {
            return builder;
        }
        TypeHandler typeHandler = TypeHandlerFactory.getTypeHandler(fieldDescriptor);
        if (data != null) {
            try {
                builder = typeHandler.transformToProtoBuilder(builder, data);
            } catch (IllegalArgumentException e) {
                String protoType = fieldDescriptor.getType().toString();
                if (fieldDescriptor.isRepeated()) {
                    protoType = String.format("REPEATED %s", fieldDescriptor.getType());
                }
                String errMessage = String.format("column invalid: type mismatch of column %s, expecting %s type. Actual type %s", fieldDescriptor.getName(), protoType, data.getClass());
                throw new InvalidColumnMappingException(errMessage, e);
            }
        }

        return builder;
    }
}
