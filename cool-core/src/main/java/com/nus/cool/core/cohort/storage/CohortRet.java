package com.nus.cool.core.cohort.storage;

import com.google.common.base.Preconditions;
import com.nus.cool.core.cohort.CohortResultLayout;
import com.nus.cool.core.cohort.ageselect.AgeSelectionLayout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.json.JSONObject;

/**
 * Class for Cohort Analysis Result We consider the Cohort Analysis Result as a
 * x-y Axis x-Axis is
 * age, y-Axis is value, legend is cohort group CohortResult class is generated
 * according to the
 * ageselection.
 */
public class CohortRet {

  private final HashMap<String, Xaxis> cohortToValueList;

  private int min;

  private int max;

  private int interval;

  private int size;

  private class UserList {
    Set<String> added = new HashSet<>();
    List<String> userSequence = new LinkedList<>();

    int size() {
      return userSequence.size();
    }

    void add(String user) {
      if (!added.contains(user)) {
        added.add(user);
        userSequence.add(user);
      }
    }
  }

  private final Map<String, UserList> cohortToUserIdList = new HashMap<>();

  /**
   * Create a cohort ret with ageSelection.
   */
  public CohortRet(AgeSelectionLayout ageSelection) {
    this.cohortToValueList = new HashMap<>();
    this.interval = ageSelection.getInterval();
    this.min = ageSelection.getMin();
    this.max = ageSelection.getMax();
    this.size = (this.max - this.min) / this.interval + 1;
  }

  /**
   * Create a cohort ret without ageSelection.
   */
  public CohortRet() {
    this.cohortToValueList = new HashMap<>();
    this.interval = 1;
    this.min = 0;
    this.max = 0;
    this.size = 1;
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
   * Initialize the missing instance Get the certain RetUnit, user can modify
   * RetUnit in place.
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
   */
  private int getCohortAge(int index) {
    Preconditions.checkArgument(index <= max && index >= min,
        "input tuple didn't pass the ageSelection " + index + "min: " + min + "max: " + max);
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

    // TODO(lingze), only support int type value
    public List<Integer> getValues() {
      ArrayList<Integer> ret = new ArrayList<>();
      for (int i = 0; i < retUnits.length; i++) {
        if (retUnits[i] == null) {
          ret.add(0);
          continue;
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

  /**
   * addUserid.
   *
   * @param cohortName cohortName
   * @param userId     userId
   */
  public void addUserid(String cohortName, String userId) {
    if (!this.cohortToUserIdList.containsKey(cohortName)) {
      this.cohortToUserIdList.put(cohortName, new UserList());
    }
    this.cohortToUserIdList.get(cohortName).add(userId);
  }

  /**
   * After processing each cublet, clear the cohortToUserIdList mapper.
   */
  public void clearUserIds() {
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

  /**
   * Prepare query result object that contains the mapping of cohort name and size.
   */
  public CohortResultLayout genResult() {
    CohortResultLayout ret = new CohortResultLayout();
    for (Map.Entry<String, UserList> e : this.cohortToUserIdList.entrySet()) {
      ret.addOneCohortRes(e.getKey(), e.getValue().size());
    }
    return ret;
  }

  /**
   * Prepare a cohort write store that is used to persist cohort user list.
   *
   * @param cohortName the picked cohort
   * @return cohort write store that contains the selected cohort users.
   */
  public Optional<CohortWSStr> genCohortUser(String cohortName) {
    return Optional.of(this.cohortToUserIdList.get(cohortName))
      .map(x -> {
        if (x.size() == 0) {
          return null;
        } else {
          CohortWSStr c = new CohortWSStr();
          c.addCubletResults(x.userSequence);
          return c;
        }
      }); 
  }


  /**
   * Prepare cohort write stores that for all non-empty cohort user list.
   */
  public Map<String, Optional<CohortWSStr>> genAllCohortUsers() {
    return this.cohortToUserIdList.entrySet()
               .stream()
               .collect(Collectors.toMap(x -> x.getKey(),
                  x -> {
                    if (x.getValue().size() == 0) {
                      return Optional.of(null);
                    } else {
                      CohortWSStr c = new CohortWSStr();
                      c.addCubletResults(x.getValue().userSequence);
                      return Optional.of(c);
                    }
                  }));
  }

  @Override
  public String toString() {
    String ret =
        "CohortRet [interval="
            + interval
            + ", max="
            + max
            + ", min="
            + min
            + ", size="
            + size
            + "]\n";
    for (Entry<String, Xaxis> entry : this.cohortToValueList.entrySet()) {
      ret += entry.getKey() + ":" + entry.getValue().toString() + "\n";
    }
    return ret;
  }

  /**
   * return the JSON format.
   */
  public JSONObject toJson() {
    HashMap<String, Object> out = new HashMap<>();
    HashMap<String, Integer> format = new HashMap<>();
    format.put("interval", interval);
    format.put("max", max);
    format.put("min", min);
    format.put("size", size);
    out.put("format", format);
    HashMap<String, List<Integer>> results = new HashMap<>();
    for (Entry<String, Xaxis> entry : this.cohortToValueList.entrySet()) {
      results.put(entry.getKey(), entry.getValue().getValues());
    }
    out.put("results", results);
    JSONObject obj = new JSONObject(out);
    return obj;
  }
}
