package io.odpf.dagger.core.sink;

import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.core.exception.DaggerSerializationException;
import io.odpf.dagger.common.exceptions.DescriptorNotFoundException;
import io.odpf.dagger.core.exception.InvalidColumnMappingException;
import io.odpf.dagger.core.protohandler.ProtoHandler;
import io.odpf.dagger.core.protohandler.ProtoHandlerFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Objects;

public class ProtoSerializer implements KafkaSerializationSchema<Row> {
    private String[] columnNames;
    private StencilClientOrchestrator stencilClientOrchestrator;
    private String keyProtoClassName;
    private String messageProtoClassName;
    private String outputTopic;


    public ProtoSerializer(String keyProtoClassName, String messageProtoClassName, String[] columnNames, StencilClientOrchestrator stencilClientOrchestrator) {
        if (Objects.isNull(messageProtoClassName)) {
            throw new DaggerSerializationException("messageProtoClassName is required");
        }
        this.keyProtoClassName = keyProtoClassName;
        this.messageProtoClassName = messageProtoClassName;
        this.columnNames = columnNames;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
    }

    public ProtoSerializer(String keyProtoClassName, String messageProtoClassName, String[] columnNames, StencilClientOrchestrator stencilClientOrchestrator, String outputTopic) {
        this(keyProtoClassName, messageProtoClassName, columnNames, stencilClientOrchestrator);
        this.outputTopic = outputTopic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Row row, @Nullable Long aLong) {
        if (Objects.isNull(outputTopic) || outputTopic.equals("")) {
            throw new DaggerSerializationException("outputTopic is required");
        }
        byte[] key = serializeKey(row);
        byte[] message = serializeValue(row);
        return new ProducerRecord<>(outputTopic, key, message);
    }

    public byte[] serializeKey(Row row) {
        return (Objects.isNull(keyProtoClassName) || keyProtoClassName.equals("")) ? null
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
        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(fieldDescriptor);
        if (data != null) {
            try {
                builder = protoHandler.transformForKafka(builder, data);
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

    private Descriptors.Descriptor getDescriptor(String className) {
        Descriptors.Descriptor dsc = stencilClientOrchestrator.getStencilClient().get(className);
        if (dsc == null) {
            throw new DescriptorNotFoundException();
        }
        return dsc;
    }

}
