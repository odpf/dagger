package com.gojek.daggers.postProcessors.common;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public interface Validator {

    HashMap<String, Object> getMandatoryFields();

    default void validateFields() throws IllegalArgumentException {
        ArrayList fieldsMissing = new ArrayList<>();
        ArrayList<Object> nestedFields = new ArrayList<>();
        getMandatoryFields().forEach((key, value) -> {
            if (value == null || StringUtils.isEmpty(String.valueOf(value)))
                fieldsMissing.add(key);
            if (value instanceof Map){
                if(((Map) value).isEmpty())
                    fieldsMissing.add(key);
                nestedFields.addAll(((Map) value).values());
            }
        });
        if (fieldsMissing.size() != 0)
            throw new IllegalArgumentException("Missing required fields: " + fieldsMissing.toString());

        nestedFields.forEach(field -> {
            if (field instanceof Validator)
                ((Validator) field).validateFields();
        });
    }
}
