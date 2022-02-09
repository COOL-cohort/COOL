/**
 * 
 */
package com.nus.cool.core.cohort;

import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.schema.CubeSchema;
import com.nus.cool.core.schema.TableSchema;

import java.io.Closeable;

/**
 * The base interface for all cohort query operators. For the
 * current implementation, we only have two operators:
 * TableFilter and CohortBy.
 * 
 *
 */
public interface CohortOperator extends Closeable, Operator {

	void init(TableSchema tableSchema, InputVector cohortUsers, ExtendedCohortQuery query);
	
	boolean isCohortsInCublet();
	
}
