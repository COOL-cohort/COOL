package com.nus.cool.core.util;

import java.nio.ShortBuffer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ShortBuffers {

    /**
     * Search index of key by binary search
     *
     * @param buffer data
     * @param fromIndex from index in buffer
     * @param toIndex to index in buffer
     * @param key search param
     * @return index of key in buffer
     */
    public static int binarySearchUnsigned(ShortBuffer buffer, int fromIndex, int toIndex, short key) {
        checkNotNull(buffer);
        checkArgument(fromIndex < buffer.limit() && toIndex <= buffer.limit());

        int ikey= key & 0xFFFF;
        toIndex--;
        while (fromIndex <= toIndex) {
            int mid = (fromIndex + toIndex) >> 1;
            int e = buffer.get(mid) & 0xFFFF;
            if (ikey > e)
                fromIndex = mid + 1;
            else if (ikey < e)
                toIndex = mid - 1;
            else
                return mid;
        }
        return ~fromIndex;
    }
}
