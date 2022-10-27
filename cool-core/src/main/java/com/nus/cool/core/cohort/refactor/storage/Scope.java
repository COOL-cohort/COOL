package com.nus.cool.core.cohort.refactor.storage;

import lombok.Getter;

/**
 * Represent a range from left to right [left, right).
 */
public class Scope {

  @Getter
  private Integer left;
  
  @Getter
  private Integer right;

  public Scope(Integer l, Integer r) {
    this.left = l == null ? Integer.MIN_VALUE : l;
    this.right = r == null ? Integer.MAX_VALUE : r;
  }

  public Boolean isInScope(Integer i) {
    return i < this.right && i >= this.left;
  }
  
  /**
   * Check if two scopes intersect.
   */
  public Boolean isIntersection(Scope scope) {
    return !(scope.getLeft() >= this.right || scope.getRight() <= this.left);
  }

  public Boolean isSubset(Scope scope){
    return scope.getLeft() >= this.left && scope.getRight() <= this.right;
}

  @Override
  public String toString() {
    String l = this.left == Integer.MIN_VALUE ? "MIN" : this.left.toString();
    String r = this.right == Integer.MAX_VALUE ? "MAX" : this.right.toString();
    return l + "-" + r;
  }
}
