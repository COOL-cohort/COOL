package com.nus.cool.core.cohort.refactor.storage;

import java.io.IOException;
import java.util.HashMap;

import com.google.common.base.Preconditions;
import com.nus.cool.core.cohort.refactor.ageSelect.AgeSelection;

import javafx.util.Pair;

/**
 * Class for Cohort Analysis Result
 * We consider the Cohort Analysis Result as a x-y Axis
 * x-Axis is age, y-Axis is value, legend is cohort group
 * CohortResult class is generated according to the ageselection.
 */
public class CohortRet {

    private HashMap<String, Xaxis> cohortToValueList;

    private int min, max, interval;

    private int size;

    public CohortRet(AgeSelection ageSelection) {
        this.cohortToValueList = new HashMap<>();
        this.interval = ageSelection.getInterval();
        this.min = ageSelection.getMin();
        this.max = ageSelection.getMax();
        this.size = (this.max - this.min) / this.interval + 1;
    }

    public void put(String cohort, int age, int value) {
        if (!this.cohortToValueList.containsKey(cohort)) {
            this.cohortToValueList.put(cohort, new Xaxis(this.size));
        }
        int offset = calIndex(age);
        Xaxis xaxis = this.cohortToValueList.get(cohort);
        xaxis.put(offset, value);
    }

    public Pair<Integer, Integer> get(String cohort, int age) {
        if (this.cohortToValueList.containsKey(cohort)) {
            int offset = calIndex(age);
            return this.cohortToValueList.get(cohort).get(offset);
        }
        return new Pair<Integer, Integer>(0, 0);
    }

    /**
     * when invoked this func, the tuple should pass the ageSelection, index is in
     * range.
     * 
     * @param index
     * @return
     * @throws IOException
     */
    private int calIndex(int index) {
        Preconditions.checkArgument(index <= max && index >= min, "input tuple didn't pass the ageSelection");
        int offset = (index - this.min) / this.interval;
        return offset;
    }

    private class Xaxis {
        private int[] value;
        private int[] count;

        Xaxis(int size) {
            this.value = new int[size];
            this.count = new int[size];
        }

        public Pair<Integer, Integer> get(int i) {
            return new Pair<>(this.value[i], this.count[i]);
        }

        public void put(int i, int v) {
            this.value[i] = v;
            this.count[i] += 1;
        }

        // public void put(int i, Pair<Integer, Integer> v) {
        //     this.value[i] = v.getKey();
        //     this.count[i] = v.getValue();
        // }
    }

}
