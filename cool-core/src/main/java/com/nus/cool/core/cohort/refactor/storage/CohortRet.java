package com.nus.cool.core.cohort.refactor.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import com.google.common.base.Preconditions;
import com.nus.cool.core.cohort.refactor.ageSelect.AgeSelectionLayout;
import com.nus.cool.core.cohort.refactor.aggregate.AggregateFunc;


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

    private AggregateFunc func;

    public CohortRet(AgeSelectionLayout ageSelection) {
        this.cohortToValueList = new HashMap<>();
        this.interval = ageSelection.getInterval();
        this.min = ageSelection.getMin();
        this.max = ageSelection.getMax();
        this.size = (this.max - this.min) / this.interval + 1;
    }

    public CohortRet(int min, int max, int interval){
        this.cohortToValueList = new HashMap<>();
        this.min = min;
        this.max = max;
        this.interval = interval;
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
    public RetUnit getByAge(String cohort, int age) {
        if (!this.cohortToValueList.containsKey(cohort)) {
            this.cohortToValueList.put(cohort, new Xaxis(this.size));
        }
        int offset = getCohortAge(age);
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
    private int getCohortAge(int index) {
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
            for(int i = 0 ; i < retUnits.length; i++){
                if(retUnits[i] == null) {
                    ret.add(0);
                    continue;
                }
                ret.add((int)retUnits[i].getValue());
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
