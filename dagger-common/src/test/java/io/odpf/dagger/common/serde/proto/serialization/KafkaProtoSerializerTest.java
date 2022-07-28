package io.odpf.dagger.common.serde.proto.serialization;

import io.odpf.dagger.common.exceptions.serde.DaggerSerializationException;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class KafkaProtoSerializerTest {

    @Test
    public void shouldSerializeIntoKafkaRecord() {
        ProtoSerializer serializer = Mockito.mock(ProtoSerializer.class);
        String outputTopic = "test";
        Row element = new Row(1);
        element.setField(0, "testing");
        byte[] keyBytes = "key".getBytes();
        byte[] valueBytes = "value".getBytes();
        Mockito.when(serializer.serializeKey(element)).thenReturn(keyBytes);
        Mockito.when(serializer.serializeValue(element)).thenReturn(valueBytes);
        KafkaProtoSerializer kafkaProtoSerializer = new KafkaProtoSerializer(serializer, outputTopic);
        ProducerRecord<byte[], byte[]> record = kafkaProtoSerializer.serialize(element, null, null);
        ProducerRecord<byte[], byte[]> expectedRecord = new ProducerRecord<>("test", keyBytes, valueBytes);
        Assert.assertEquals(expectedRecord, record);
    }

    @Test
    public void shouldThrowExceptionWhenOutputTopicIsNullForSerializeMethod() {
        ProtoSerializer serializer = Mockito.mock(ProtoSerializer.class);
        KafkaProtoSerializer kafkaProtoSerializer = new KafkaProtoSerializer(serializer, null);
        Row element = new Row(1);
        element.setField(0, "1234");
        DaggerSerializationException exception = assertThrows(DaggerSerializationException.class,
                () -> kafkaProtoSerializer.serialize(element, null, System.currentTimeMillis() / 1000));
        assertEquals("outputTopic is required", exception.getMessage());
    }

    @Test
    public void shouldThrowExceptionWhenOutputTopicIsEmptyForSerializeMethod() {
        ProtoSerializer serializer = Mockito.mock(ProtoSerializer.class);
        KafkaProtoSerializer kafkaProtoSerializer = new KafkaProtoSerializer(serializer, "");
        Row element = new Row(1);
        element.setField(0, "1234");

        DaggerSerializationException exception = assertThrows(DaggerSerializationException.class,
                () -> kafkaProtoSerializer.serialize(element, null, System.currentTimeMillis() / 1000));
        assertEquals("outputTopic is required", exception.getMessage());
    }
}
