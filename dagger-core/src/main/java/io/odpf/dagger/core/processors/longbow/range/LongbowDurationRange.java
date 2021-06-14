package io.odpf.dagger.core.processors.longbow.range;

import io.odpf.dagger.core.processors.longbow.LongbowSchema;
import io.odpf.dagger.core.utils.Constants;

import org.apache.flink.types.Row;

public class LongbowDurationRange implements LongbowRange {
    private LongbowSchema longbowSchema;

    public LongbowDurationRange(LongbowSchema longbowSchema) {
        this.longbowSchema = longbowSchema;
    }

    @Override
    public byte[] getUpperBound(Row input) {
        return longbowSchema.getKey(input, 0);
    }

    @Override
    public byte[] getLowerBound(Row input) {
        return longbowSchema.getKey(input, longbowSchema.getDurationInMillis(input));
    }

    @Override
    public String[] getInvalidFields() {
        return new String[]{Constants.LONGBOW_EARLIEST_KEY, Constants.LONGBOW_LATEST_KEY};
    }
}
