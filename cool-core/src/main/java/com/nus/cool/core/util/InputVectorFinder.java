package com.nus.cool.core.util;

import static com.google.common.base.Preconditions.checkArgument;

public class InputVectorFinder<T extends Comparable<T>> {

  int curPos;

  T[] array;
  
  public InputVectorFinder(T[] array) {
    this.curPos = 0;
    this.array = array;
  }

  /**
   * Binary search the array for the matching element.
   */
  public int binarySearch(int fromIndex, int toIndex, T key) {
    checkArgument(fromIndex <= array.length && toIndex <= array.length);
    
    int mid = (fromIndex + toIndex) >> 1;
    if (key.compareTo(array[mid]) > 0) {
      fromIndex = mid + 1;
    } else if (key.compareTo(array[mid]) < 0) {
      toIndex = mid - 1;
    } else {
      return mid;
    }
    return ~fromIndex;
  }

  /**
   * Traverse the array and find the matching element.
   */
  public int traverseSearch(int fromIndex, int toIndex, T key) {
    checkArgument(fromIndex <= array.length && toIndex <= array.length);

    for (int i = fromIndex; i < toIndex; i++) {
      if (key.compareTo(array[i]) == 0) {
        return i;
      }
    }
    return -1;
  }
}
