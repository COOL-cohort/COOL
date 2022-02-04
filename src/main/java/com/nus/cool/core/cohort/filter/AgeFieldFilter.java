/**
 * 
 */
package com.nus.cool.core.cohort.filter;

import com.nus.cool.core.cohort.ExtendedFieldSet;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 *
 */
public class AgeFieldFilter implements FieldFilter {
	
	private int minAge;
	
	private int maxAge;
	
	public AgeFieldFilter(List<String> values) {
		checkArgument(values != null && values.isEmpty() == false);
		String[] range = values.get(0).split("\\|");
		this.minAge = Integer.parseInt(range[0]);
		this.maxAge = Integer.parseInt(range[1]);
	}

	@Override
	public int getMinKey() {
		return minAge;
	}

	@Override
	public int getMaxKey() {
		return maxAge;
	}

	@Override
	public boolean accept(MetaFieldRS metaField) {
		return true;
	}

	@Override
	public boolean accept(FieldRS chunkField) {
		return true;
	}

	@Override
	public boolean accept(int v) {
		return (v >= minAge && v <= maxAge);
	}

	@Override
	public int nextAcceptTuple(int start, int to) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getNumKey() {
		// TODO Auto-generated method stub
		return 0;

	}

	@Override
	public ExtendedFieldSet getFieldSet() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateValues(Double v) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean accept(Double v) {
		throw new UnsupportedOperationException();
	}

}
