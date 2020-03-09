package com.gojek.daggers.postProcessors.longbow.range;

import org.apache.flink.types.Row;

import java.io.Serializable;

public interface LongbowRange extends Serializable {
    byte[] getUpperBound(Row input);

    byte[] getLowerBound(Row input);

    String[] getInvalidFields();
}
