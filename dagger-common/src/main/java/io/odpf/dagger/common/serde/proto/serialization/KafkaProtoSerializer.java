package io.odpf.dagger.common.serde.proto.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.exceptions.serde.DaggerSerializationException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class KafkaProtoSerializer implements KafkaRecordSerializationSchema<Row> {
    private final String outputTopic;
    private final ProtoSerializer protoSerializer;
    private static final Logger LOGGER = LoggerFactory.getLogger("KafkaSink");

    public KafkaProtoSerializer(ProtoSerializer protoSerializer) {
        this(protoSerializer, "");
    }

    public KafkaProtoSerializer(ProtoSerializer protoSerializer, String outputTopic) {
        this.protoSerializer = protoSerializer;
        this.outputTopic = outputTopic;
    }

    @Override
    public void open(InitializationContext context, KafkaSinkContext sinkContext) throws Exception {
        KafkaRecordSerializationSchema.super.open(context, sinkContext);
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Row row, KafkaSinkContext context, Long timestamp) {
        if (Objects.isNull(outputTopic) || outputTopic.equals("")) {
            throw new DaggerSerializationException("outputTopic is required");
        }
        LOGGER.info("row to kafka: " + row);
        byte[] key = protoSerializer.serializeKey(row);
        byte[] message = protoSerializer.serializeValue(row);
        return new ProducerRecord<>(outputTopic, key, message);
    }
}
