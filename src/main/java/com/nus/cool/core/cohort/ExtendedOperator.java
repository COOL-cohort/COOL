package com.nus.cool.core.cohort;

import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.schema.TableSchema;

import java.io.Closeable;

public interface ExtendedOperator extends Closeable, Operator {

    void init(CubeSchema cubeSchema, TableSchema tableSchema, InputVector cohortUsers, ExtendedCohortQuery query);

    boolean isCohortsInCublet();

}
