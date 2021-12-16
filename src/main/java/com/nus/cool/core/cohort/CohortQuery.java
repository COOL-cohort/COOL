package com.nus.cool.core.cohort;

import com.google.common.collect.Lists;
import lombok.Data;

import java.util.List;

@Data
public class CohortQuery {

    private String dataSource;

    private String appKey;

    private String[] birthActions;

    private String[] cohortFields;

    private List<FieldSet> birthSelection = Lists.newArrayList();

    private List<FieldSet> ageSelection = Lists.newArrayList();

    private int ageInterval = 1;

    private String metric;

    private String outSource;
}
