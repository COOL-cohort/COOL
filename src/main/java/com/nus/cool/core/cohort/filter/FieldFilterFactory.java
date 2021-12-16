package com.nus.cool.core.cohort.filter;

import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.util.converter.DayIntConverter;
import com.nus.cool.core.util.converter.StringIntConverter;

import java.util.List;

public class FieldFilterFactory {

    public static FieldFilter create(FieldSchema schema, List<String> values) {
        switch (schema.getFieldType()) {
            case AppKey:
            case UserKey:
            case Segment:
            case Action:
                return new SetFieldFilter(values);
            case ActionTime:
                return new RangeFieldFilter(values, new DayIntConverter());
            case Metric:
                return new RangeFieldFilter(values, new StringIntConverter());
            default:
                throw new IllegalArgumentException("Unsupported field type: " + schema.getFieldType());
        }
    }
}
