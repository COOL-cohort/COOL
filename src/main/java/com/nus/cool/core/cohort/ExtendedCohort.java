package com.nus.cool.core.cohort;

import java.util.ArrayList;
import java.util.List;


/**
 * @file Cohort.java
 * @brief
 * @version
 */
public class ExtendedCohort {
    
    private List<Integer> dimensions = new ArrayList<>();
    
    private List<String> dimentionNames = new ArrayList<>();

    private int birthOffset;

    private int birthDate;

    public ExtendedCohort() {
        birthOffset = -1;
    }

    public ExtendedCohort(ExtendedCohort that) {
        this.dimensions.addAll(that.dimensions);
        this.birthOffset = that.birthOffset;
    }

    public void addDimension(int dim) {
        this.dimensions.add(dim);
    }

    public void clearDimension() {
        this.dimensions.clear();
    }

    public void addDimensionName(String name) {
        this.dimentionNames.add(name);
    }

    /**
     * @return the dimensions
     */
    public List<Integer> getDimensions() {
        return dimensions;
    }

    /**
     * @return the birthOffset
     */
    public int getBirthOffset() {
        return birthOffset;
    }

    /**
     * @param birthOffset the birth to set
     */
    public void setBirthOffset(int birthOffset) {
        this.birthOffset = birthOffset;
    }

    /**
     * @return the birthDate
     */
    public int getBirthDate() {
        return birthDate;
    }

    /**
     * @param birthDate the birthDate to set
     */
    public void setBirthDate(int birthDate) {
        this.birthDate = birthDate;
    }

    @Override
    public int hashCode() {
        return this.dimensions.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return this.dimensions.equals(((ExtendedCohort)obj).dimensions);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append('(');
//        for (int dim : this.dimensions) {
//        	builder.append(dim);
//        	builder.append(", ");
//        }
        for (String dimName : this.dimentionNames) {
            builder.append(dimName);
            builder.append(", ");
        }
        if(!this.dimensions.isEmpty()) {
        	if (builder.length() >= 2)
        		builder.delete(builder.length() - 2, builder.length());
        }
        else
            builder.append("cohort");
        builder.append(')');
        return builder.toString();
    }
}
