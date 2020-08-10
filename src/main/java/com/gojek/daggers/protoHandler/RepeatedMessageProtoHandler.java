package com.gojek.daggers.protoHandler;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.DynamicMessage.Builder;
import net.minidev.json.JSONArray;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE;

public class RepeatedMessageProtoHandler implements ProtoHandler {
    private FieldDescriptor fieldDescriptor;

    public RepeatedMessageProtoHandler(FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == MESSAGE && fieldDescriptor.isRepeated();
    }

    @Override
    public Builder transformForKafka(Builder builder, Object field) {
        if (!canHandle() || field == null) {
            return builder;
        }

        ArrayList<DynamicMessage> messages = new ArrayList<>();
        List<FieldDescriptor> nestedFieldDescriptors = fieldDescriptor.getMessageType().getFields();

        if (field instanceof ArrayList) {
            ArrayList<Object> rowElements = (ArrayList<Object>) field;
            for (Object row : rowElements) {
                messages.add(getNestedDynamicMessage(nestedFieldDescriptors, (Row) row));
            }
        } else {
            Object[] rowElements = (Object[]) field;
            for (Object row : rowElements) {
                messages.add(getNestedDynamicMessage(nestedFieldDescriptors, (Row) row));
            }
        }
        return builder.setField(fieldDescriptor, messages);
    }

    @Override
    public Object transformFromPostProcessor(Object field) {
        ArrayList<Row> rows = new ArrayList<>();
        if (field != null) {
            Object[] inputFields = ((JSONArray) field).toArray();
            for (Object inputField : inputFields) {
                rows.add(RowFactory.createRow((Map<String, Object>) inputField, fieldDescriptor.getMessageType()));
            }
        }
        return rows.toArray();
    }

    @Override
    public Object transformFromKafka(Object field) {
        ArrayList<Row> rows = new ArrayList<>();
        if (field != null) {
            List<DynamicMessage> protos = (List<DynamicMessage>) field;
            protos.forEach(proto -> rows.add(RowFactory.createRow(proto)));
        }
        return rows.toArray();
    }

    @Override
    public TypeInformation getTypeInformation() {
        return Types.OBJECT_ARRAY(TypeInformationFactory.getRowType(fieldDescriptor.getMessageType()));
    }

    private DynamicMessage getNestedDynamicMessage(List<FieldDescriptor> nestedFieldDescriptors, Row row) {
        Builder elementBuilder = DynamicMessage.newBuilder(fieldDescriptor.getMessageType());
        handleNestedField(elementBuilder, nestedFieldDescriptors, row);
        return elementBuilder.build();
    }

    private void handleNestedField(Builder elementBuilder, List<FieldDescriptor> nestedFieldDescriptors, Row row) {
        for (FieldDescriptor nestedFieldDescriptor : nestedFieldDescriptors) {
            int index = nestedFieldDescriptor.getIndex();

            if (index < row.getArity()) {
                ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(nestedFieldDescriptor);
                protoHandler.transformForKafka(elementBuilder, row.getField(index));
            }
        }
    }
}
