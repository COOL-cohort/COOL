package com.nus.cool.core.util;

/**
 * Missing functions in java Array
 */
public class ArrayUtil {

    /**
     * Find the max value in int array
     *
     * @param vec target int array
     * @return max value
     */
    public static int max(int[] vec) {
        int max = Integer.MIN_VALUE;
        for (int v : vec) {
            max = Math.max(max, v);
        }
        return max;
    }

    /**
     * Find the min value in int array
     *
     * @param vec target int array
     * @return min value
     */
    public static int min(int[] vec) {
        int min = Integer.MAX_VALUE;
        for (int v : vec) {
            min = Math.min(min, v);
        }
        return min;
    }
}
