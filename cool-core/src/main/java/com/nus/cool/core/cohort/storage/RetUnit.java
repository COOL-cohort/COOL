package com.nus.cool.core.cohort.storage;

import java.util.HashSet;
import lombok.Data;

/**
 * RetUnit to bind value and count. Provide get and set interface
 */
@Data
public class RetUnit {
  // intermediate value for cohort analysis
  private float value;

  // how many action tuple belong to this selection
  // (used to calculate AVERAGE and COUNT)
  private int count;

  private boolean used;

  private HashSet<Integer> userIdSet;

  /**
   * Create a RetUnit with initial value and count.
   */
  public RetUnit(float value, int count) {
    this.value = value;
    this.count = count;
    this.used = false;
    userIdSet = new HashSet<>();
  }

  @Override
  public String toString() {
    Integer i = (int) this.value;
    return i.toString();
  }

}
