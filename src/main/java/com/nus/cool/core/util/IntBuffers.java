package com.nus.cool.core.util;

import java.nio.IntBuffer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class IntBuffers {

    public static int binarySearch(IntBuffer buffer, int fromIndex, int toIndex, int key) {
        checkNotNull(buffer);
        checkArgument(fromIndex < buffer.limit() && toIndex <= buffer.limit());

        toIndex--;
        while (fromIndex <= toIndex) {
            int mid = (fromIndex + toIndex) >> 1;
            int e = buffer.get(mid);
            if (key > e)
                fromIndex = mid + 1;
            else if (key < e)
                toIndex = mid - 1;
            else
                return mid;
        }
        return ~fromIndex;
    }
}
