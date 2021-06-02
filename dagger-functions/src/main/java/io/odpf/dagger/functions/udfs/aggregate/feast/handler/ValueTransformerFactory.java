package io.odpf.dagger.functions.udfs.aggregate.feast.handler;

import java.util.Arrays;
import java.util.List;

public class ValueTransformerFactory {
    public static List<ValueTransformer> getValueTransformers() {
        return Arrays.asList(new BooleanValueTransformer(),
                new ByteValueTransformer(),
                new DoubleValueTransformer(),
                new FloatValueTransformer(),
                new IntegerValueTransformer(),
                new LongValueTransformer(),
                new StringValueTransformer(),
                new TimestampValueTransformer(),
                new NullValueTransformer(),
                new BigDecimalValueTransformer());
    }
}
