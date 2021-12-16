package com.nus.cool.core.util;

import java.nio.ByteOrder;

/**
 * Missing functions for java.lang.Integer
 */
public class IntegerUtil {

    /**
     * Find the minimum number of bits to represent the integer
     *
     * @param i the integer
     * @return the number of bits
     */
    public static int minBits(int i) {
        i = (i == 0 ? 1 : i);
        return Integer.SIZE - Integer.numberOfLeadingZeros(i);
    }

    /**
     * Find the minimum number of bytes to represent the integer
     *
     * @param i the integer
     * @return the number of bytes
     */
    public static int minBytes(int i) {
        return ((minBits(i) - 1) >>> 3) + 1;
    }

    /**
     * Convert the input value into OS's native byte order
     *
     * @param i the integer
     * @return the @param i in native order
     */
    public static int toNativeByteOrder(int i) {
        boolean bLittle = (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN);
        return (bLittle ? Integer.reverseBytes(i) : i);
    }
}
