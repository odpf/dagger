package io.odpf.dagger.functions.udfs.aggregate.feast;

import io.odpf.dagger.functions.udfs.aggregate.feast.handler.ValueEnum;
import io.odpf.dagger.functions.udfs.aggregate.feast.handler.ValueTransformer;
import io.odpf.dagger.functions.udfs.aggregate.feast.handler.ValueTransformerFactory;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class FeatureUtils {
    public static void populateFeatures(ArrayList<Row> featureRows, String key, Object value, Integer featureRowLength) {
        List<ValueTransformer> valueTransformers = ValueTransformerFactory.getValueTransformers();
        Optional<ValueTransformer> valueHandler = valueTransformers
                .stream()
                .filter(valueTransformer -> valueTransformer.canTransform(value))
                .findFirst();
        if (!valueHandler.isPresent()) {
            throw new IllegalArgumentException();
        }
        Row featureRow = new Row(featureRowLength);
        featureRow.setField(0, key);
        featureRow.setField(1, valueHandler.get().transform(value));
        featureRow.setField(2, key);
        featureRows.add(featureRow);
    }

    public static void populateFeaturesWithType(ArrayList<Row> featureRows, String key, Object value, ValueEnum type, Integer featureRowLength) {
        List<ValueTransformer> valueTransformers = ValueTransformerFactory.getValueTransformers();

        Optional<ValueTransformer> valueHandler = valueTransformers
                .stream()
                .filter(valueTransformer -> valueTransformer.canTransformWithTargetType(value, type))
                .findAny();

        if (!valueHandler.isPresent()) {
            throw new IllegalArgumentException();
        }

        Row featureRow = new Row(featureRowLength);
        featureRow.setField(0, key);
        featureRow.setField(1, valueHandler.get().transform(value));
        featureRow.setField(2, key);
        featureRows.add(featureRow);
    }
}
