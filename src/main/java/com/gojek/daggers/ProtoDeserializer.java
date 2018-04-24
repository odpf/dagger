package com.gojek.daggers;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;


public class ProtoDeserializer implements KeyedDeserializationSchema<Row> {

    private static final String PROTO_CLASS_MISCONFIGURED_ERROR = "proto class is misconfigured";
    private String protoClassName;
    private ProtoType protoType;
    private int timestampFieldIndex;
    private Descriptors.Descriptor descriptor;

    public ProtoDeserializer(String protoClassName, int timestampFieldIndex, String rowtimeAttributeName) {
        this.protoClassName = protoClassName;
        this.protoType = new ProtoType(protoClassName, rowtimeAttributeName);
        this.timestampFieldIndex = timestampFieldIndex;
    }

    Descriptors.Descriptor createProtoParser() {
        try {
            Class<?> protoClass = Class.forName(protoClassName);
            return (Descriptors.Descriptor) protoClass.getMethod("getDescriptor").invoke(null);
        } catch (ReflectiveOperationException exception) {
            throw new DaggerConfigurationException(PROTO_CLASS_MISCONFIGURED_ERROR, exception);
        }
    }

    private Descriptors.Descriptor getProtoParser() {
        if (descriptor == null) {
            descriptor = createProtoParser();
        }
        return descriptor;
    }

    private Row getRow(DynamicMessage proto) {
        List<Descriptors.FieldDescriptor> fields = proto.getDescriptorForType().getFields();
        Row row = new Row(fields.size());
        for (Descriptors.FieldDescriptor field : fields) {
            if (field.isMapField()) {
                List<DynamicMessage> mapEntries = (List<DynamicMessage>) proto.getField(field);
                row.setField(field.getIndex(), getMapRow(mapEntries));
                continue;
            }

            if (field.getType() == Descriptors.FieldDescriptor.Type.ENUM) {
                row.setField(field.getIndex(), proto.getField(field).toString());
            } else if (field.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
                if (field.isRepeated()) {
                    row.setField(field.getIndex(), getRow((List<DynamicMessage>) proto.getField(field)));
                } else {
                    row.setField(field.getIndex(), getRow((DynamicMessage) proto.getField(field)));
                }
            } else {
                if (field.isRepeated()) {
                    if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.STRING) {
                        row.setField(field.getIndex(), getRowForStringList((List<String>) proto.getField(field)));
                    }
                } else {
                    row.setField(field.getIndex(), proto.getField(field));
                }
            }
        }
        return row;
    }

    private Object[] getRow(List<DynamicMessage> protos) {
        ArrayList<Row> rows = new ArrayList<>();
        protos.forEach(generatedMessageV3 -> rows.add(getRow(generatedMessageV3)));
        return rows.toArray();
    }

    private Object[] getMapRow(List<DynamicMessage> protos) {
        ArrayList<Row> rows = new ArrayList<>();
        protos.forEach(entry -> rows.add(getRowFromMap(entry)));
        return rows.toArray();
    }

    private Row getRowFromMap(DynamicMessage protos) {
        Row row = new Row(2);
        Object[] keyValue = protos.getAllFields().values().toArray();
        row.setField(0, keyValue[0]);
        row.setField(1, keyValue[1]);
        return row;
    }

    private String[] getRowForStringList(List<String> listField) {
        String[] list = new String[listField.size()];
        for (int listIndex = 0; listIndex < listField.size(); listIndex++) {
            list[listIndex] = listField.get(listIndex);
        }
        return list;
    }


    @Override
    public Row deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
        try {
            DynamicMessage proto = DynamicMessage.parseFrom(getProtoParser(), message);
            return addTimestampFieldToRow(getRow(proto), proto);
        } catch (RuntimeException e) {
            throw new ProtoDeserializationExcpetion(e);
        }
    }

    private Row addTimestampFieldToRow(Row row, DynamicMessage proto) {
        Descriptors.FieldDescriptor fieldDescriptor = proto.getDescriptorForType().getFields().get(timestampFieldIndex);

        Row finalRecord = new Row(row.getArity() + 1);
        for (int fieldIndex = 0; fieldIndex < row.getArity(); fieldIndex++) {
            finalRecord.setField(fieldIndex, row.getField(fieldIndex));
        }
        Row timestampRow = getRow((DynamicMessage) proto.getField(fieldDescriptor));
        long timestampSeconds = (long) timestampRow.getField(0);
        long timestampNanos = (int) timestampRow.getField(1);

        finalRecord.setField(finalRecord.getArity() - 1, Timestamp.from(Instant.ofEpochSecond(timestampSeconds, timestampNanos)));
        return finalRecord;
    }

    @Override
    public boolean isEndOfStream(Row nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return protoType.getRowType();
    }
}
