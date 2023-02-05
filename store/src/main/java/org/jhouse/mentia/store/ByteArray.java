package org.jhouse.mentia.store;

import java.util.Arrays;

public class ByteArray implements Comparable<ByteArray> {
    private final byte[] arr;

    public ByteArray(byte[] arr) {
        this.arr = arr;
    }

    public byte[] get() {
        return this.arr;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ByteArray byteArray = (ByteArray) o;
        return Arrays.equals(arr, byteArray.arr);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(arr);
    }

    @Override
    public int compareTo(ByteArray o) {
        return Arrays.compare(arr, o.arr);
    }
}
