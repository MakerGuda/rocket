package org.apache.rocketmq.filter.util;

import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;

@Getter
@Setter
public class BloomFilterData {

    private int[] bitPos;

    private int bitNum;

    public BloomFilterData(int[] bitPos, int bitNum) {
        this.bitPos = bitPos;
        this.bitNum = bitNum;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (!(o instanceof BloomFilterData))
            return false;
        final BloomFilterData that = (BloomFilterData) o;
        if (bitNum != that.bitNum)
            return false;
        return Arrays.equals(bitPos, that.bitPos);
    }

    @Override
    public int hashCode() {
        int result = bitPos != null ? Arrays.hashCode(bitPos) : 0;
        result = 31 * result + bitNum;
        return result;
    }

}