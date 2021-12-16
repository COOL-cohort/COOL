package com.nus.cool.core.cohort.filter;

import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;

import java.util.List;

public interface FieldFilter {

    int getMinKey();

    int getMaxKey();

    boolean accept(MetaFieldRS metaField);

    boolean accept(FieldRS field);

    boolean accept(int v);

    List<String> getValues();
}
