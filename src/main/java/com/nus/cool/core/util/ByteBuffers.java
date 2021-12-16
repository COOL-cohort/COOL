package com.nus.cool.core.util;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ByteBuffers {

    /**
     * Search index of key by binary search
     *
     * @param buffer    data
     * @param fromIndex from index in buffer
     * @param toIndex   to index in buffer
     * @param key       search param
     * @return index of key in buffer
     */
    public static int binarySearchUnsigned(ByteBuffer buffer, int fromIndex, int toIndex, byte key) {
        checkNotNull(buffer);
        checkArgument(fromIndex < buffer.limit() && toIndex <= buffer.limit());

        int ikey = key & 0xFF;
        toIndex--;
        while (fromIndex <= toIndex) {
            int mid = (fromIndex + toIndex) >> 1;
            int e = buffer.get(mid) & 0xFF;
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
