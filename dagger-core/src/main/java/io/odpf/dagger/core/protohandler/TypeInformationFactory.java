package io.odpf.dagger.core.protohandler;

import io.odpf.dagger.common.exceptions.DescriptorNotFoundException;
import com.google.protobuf.Descriptors;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

public class TypeInformationFactory {
    public static TypeInformation<Row> getRowType(Descriptors.Descriptor descriptor) {
        if (descriptor == null) {
            throw new DescriptorNotFoundException();
        }
        String[] fieldNames = descriptor
                .getFields()
                .stream()
                .map(Descriptors.FieldDescriptor::getName)
                .toArray(String[]::new);
        TypeInformation[] fieldTypes = descriptor
                .getFields()
                .stream()
                .map(fieldDescriptor -> ProtoHandlerFactory.getProtoHandler(fieldDescriptor).getTypeInformation())
                .toArray(TypeInformation[]::new);
        return Types.ROW_NAMED(fieldNames, fieldTypes);
    }
}
