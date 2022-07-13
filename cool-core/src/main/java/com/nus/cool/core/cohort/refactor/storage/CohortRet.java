package com.nus.cool.core.cohort.refactor.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;

import com.google.common.base.Preconditions;
import com.nus.cool.core.cohort.refactor.ageSelect.AgeSelection;

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

    /**
     * Initialize the missing intance
     * Get the certain RetUnit, user can modify RetUnit in place
     * 
     * @param cohort
     * @param age
     * @return
     */
    public RetUnit get(String cohort, int age) {
        if (!this.cohortToValueList.containsKey(cohort)) {
            this.cohortToValueList.put(cohort, new Xaxis(this.size));
        }
        int offset = calIndex(age);
        return this.cohortToValueList.get(cohort).get(offset);
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

    /**
     * Typedef RetUnit[] to Xaxis
     */
    private class Xaxis {
        private RetUnit[] retUnits;

        Xaxis(int size) {
            this.retUnits = new RetUnit[size];
        }

        // Directly change the RetUnit in place
        public RetUnit get(int i) {
            if (this.retUnits[i] == null)
                this.retUnits[i] = new RetUnit(0, 0);
            return this.retUnits[i];
        }

        // TODO(lingze), only support int type value
        public List<Integer> getValues(){
            ArrayList<Integer> ret =  new ArrayList<>();
            for(RetUnit i : retUnits){
                ret.add((int)i.getValue());
            }
            return ret;
        }

        @Override
        public String toString() {
            return "Xaxis [retUnits=" + Arrays.toString(retUnits) + "]";
        }

    }

    /**
     * 
     * @return get the list of all cohortName 
     */
    public List<String> getCohortList(){
        return new ArrayList<>(this.cohortToValueList.keySet());
    }
    
    /**
     * 
     * @param cohort
     * @return
     */
    public List<Integer> getValuesByCohort(String cohort){
         if(!this.cohortToValueList.containsKey(cohort)){
            return null;
         }  
         Xaxis x = this.cohortToValueList.get(cohort);
         return x.getValues();
    }


    @Override
    public String toString() {
        String ret = "CohortRet [interval=" + interval + ", max=" + max
                + ", min=" + min + ", size=" + size + "]\n";
        for (Entry<String,Xaxis> entry:this.cohortToValueList.entrySet()){
            ret += entry.getKey() + ":" + entry.getValue().toString() + "\n";
        }
        return ret;
    }

}
