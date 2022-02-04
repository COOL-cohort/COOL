/**
 * 
 */
package com.nus.cool.core.cohort;

import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.schema.*;

import java.io.Closeable;

/**
 * The base interface for all cohort query operators. For the
 * current implementation, we only have two operators:
 * TableFilter and CohortBy.
 * 
 * @author david, qingchao
 *
 */
public interface CohortOperator extends Closeable, Operator {

	void init(CubeSchema cubeSchema, TableSchema tableSchema, InputVector cohortUsers, ExtendedCohortQuery query);


	boolean isCohortsInCublet();
	
}
