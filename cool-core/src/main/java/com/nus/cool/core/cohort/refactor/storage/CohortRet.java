package com.nus.cool.core.cohort.refactor.storage;

import com.google.common.base.Preconditions;
import com.nus.cool.core.cohort.refactor.ageselect.AgeSelectionLayout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import lombok.Getter;

/**
 * Class for Cohort Analysis Result We consider the Cohort Analysis Result as a
 * x-y Axis x-Axis is
 * age, y-Axis is value, legend is cohort group CohortResult class is generated
 * according to the
 * ageselection.
 */
public class CohortRet {

<<<<<<< HEAD
  private HashMap<String, Xaxis> cohortToValueList;

  private int min;

  private int max;

  private int interval;

  private int size;

  @Getter
  private final HashMap<String, List<String>> cohortToUserIdList = new HashMap<>();

  /**
   * Create a cohort ret.
   */
  public CohortRet(AgeSelectionLayout ageSelection) {
    this.cohortToValueList = new HashMap<>();
    this.interval = ageSelection.getInterval();
    this.min = ageSelection.getMin();
    this.max = ageSelection.getMax();
    this.size = (this.max - this.min) / this.interval + 1;
  }

  /**
   * Create a cohort ret.
   */
  public CohortRet(int min, int max, int interval) {
    this.cohortToValueList = new HashMap<>();
    this.min = min;
    this.max = max;
    this.interval = interval;
    this.size = (this.max - this.min) / this.interval + 1;
  }

  /**
   * Initialize the missing intance Get the certain RetUnit, user can modify
   * RetUnit in place.
   */
  public RetUnit getByAge(String cohort, int age) {
    if (!this.cohortToValueList.containsKey(cohort)) {
      this.cohortToValueList.put(cohort, new Xaxis(this.size));
=======
    private final HashMap<String, Xaxis> cohortToValueList;

    private int min, max, interval;

    private final int size;

    @Getter
    private final HashMap<String, List<String>> cohortToUserIdList = new HashMap<>();

    public CohortRet(AgeSelectionLayout ageSelection) {
        this.cohortToValueList = new HashMap<>();
        this.interval = ageSelection.getInterval();
        this.min = ageSelection.getMin();
        this.max = ageSelection.getMax();
        this.size = (this.max - this.min) / this.interval + 1;
>>>>>>> d88b8cc (Update cohort processor logic to support cohort support)
    }
    int offset = getCohortAge(age);
    return this.cohortToValueList.get(cohort).get(offset);
  }

  /**
   * when invoked this func, the tuple should pass the ageSelection, index is in
   * range.
   */
  private int getCohortAge(int index) {
    Preconditions.checkArgument(index <= max && index >= min,
        "input tuple didn't pass the ageSelection");
    int offset = (index - this.min) / this.interval;
    return offset;
  }

  /**
   * Typedef RetUnit[] to Xaxis.
   */
  private class Xaxis {
    private RetUnit[] retUnits;

    Xaxis(int size) {
      this.retUnits = new RetUnit[size];
    }

    // Directly change the RetUnit in place
    public RetUnit get(int i) {
      if (this.retUnits[i] == null) {
        this.retUnits[i] = new RetUnit(0, 0);
      }
      return this.retUnits[i];
    }

<<<<<<< HEAD
    // TODO(lingze), only support int type value
    public List<Integer> getValues() {
      ArrayList<Integer> ret = new ArrayList<>();
      for (int i = 0; i < retUnits.length; i++) {
        if (retUnits[i] == null) {
          ret.add(0);
          continue;
=======
    public void addUserid(String cohortName, String userId){
        if (!this.cohortToUserIdList.containsKey(cohortName)){
            List<String> userIdList = new ArrayList<>();
            this.cohortToUserIdList.put(cohortName ,userIdList);
>>>>>>> d88b8cc (Update cohort processor logic to support cohort support)
        }
        ret.add((int) retUnits[i].getValue());
      }
      return ret;
    }

    @Override
    public String toString() {
      return "Xaxis [retUnits=" + Arrays.toString(retUnits) + "]";
    }

  }

  public void addUserid(String cohortName, String userId){
    if (!this.cohortToUserIdList.containsKey(cohortName)){
      List<String> userIdList = new ArrayList<>();
      this.cohortToUserIdList.put(cohortName ,userIdList);
    }
    this.cohortToUserIdList.get(cohortName).add(userId);
  }

  /**
   * After processing each cublet, clear the cohortToUserIdList mapper,
   */
  public void ClearUserIds(){
      this.cohortToUserIdList.clear();
  }

  public List<String> getCohortList() {
    return new ArrayList<>(this.cohortToValueList.keySet());
  }

  /**
   * Get values of each age of a cohort.
   */
  public List<Integer> getValuesByCohort(String cohort) {
    if (!this.cohortToValueList.containsKey(cohort)) {
      return null;
    }
    Xaxis x = this.cohortToValueList.get(cohort);
    return x.getValues();
  }

  @Override
  public String toString() {
    String ret = "CohortRet [interval=" + interval + ", max=" + max + ", min=" + min + ", size="
        + size + "]\n";
    for (Entry<String, Xaxis> entry : this.cohortToValueList.entrySet()) {
      ret += entry.getKey() + ":" + entry.getValue().toString() + "\n";
    }
    return ret;
  }
}
