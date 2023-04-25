package com.nus.cool.core.cohort.storage;

import com.nus.cool.core.field.RangeField;
import lombok.AllArgsConstructor;

/**
 * Represent a range from left to right [left, right).
 */
@AllArgsConstructor
public class Scope {

  private RangeField left;

  private RangeField right;

  public Boolean isInScope(RangeField i) {
    return (left == null || i.compareTo(left) >= 0)
      && (right == null || i.compareTo(right) < 0);
  }

  /**
   * Check if two scopes intersect.
   */
  public Boolean isIntersection(Scope scope) {
    return !(
      (scope.left != null && right != null && scope.left.compareTo(right) >= 0) 
      || (scope.right != null && left != null && scope.right.compareTo(left) <= 0));
  }

  public Boolean isSubset(Scope scope) {
    return (left == null || (scope.left != null && scope.left.compareTo(left) >= 0)) 
      && (right == null || (scope.right != null && scope.right.compareTo(right) <= 0));
  }

  public void copy(Scope in) {
    left = in.left;
    right = in.right;
  }

  /**
   * Generate a string representation.
   */
  public String getString(Boolean useInt) {
    String l = this.left == null ? "MIN" :
        (useInt ? String.valueOf(this.left.getInt()) : this.left.toString());
    String r = this.right == null ? "MAX" :
        (useInt ? String.valueOf(this.right.getInt()) : this.right.toString());
    return l + "-" + r;
  }
}
