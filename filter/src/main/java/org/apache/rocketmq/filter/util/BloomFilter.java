package org.apache.rocketmq.filter.util;

import com.google.common.hash.Hashing;
import lombok.Getter;
import lombok.Setter;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

@Getter
@Setter
public class BloomFilter {

    public static final Charset UTF_8 = StandardCharsets.UTF_8;

    private int f;

    private int n;

    private int k;

    private int m;

    private BloomFilter(int f, int n) {
        if (f < 1 || f >= 100) {
            throw new IllegalArgumentException("f must be greater or equal than 1 and less than 100");
        }
        if (n < 1) {
            throw new IllegalArgumentException("n must be greater than 0");
        }
        this.f = f;
        this.n = n;
        double errorRate = f / 100.0;
        this.k = (int) Math.ceil(logMN(0.5, errorRate));
        if (this.k < 1) {
            throw new IllegalArgumentException("Hash function num is less than 1, maybe you should change the value of error rate or bit num!");
        }
        this.m = (int) Math.ceil(this.n * logMN(2, 1 / errorRate) * logMN(2, Math.E));
        this.m = (int) (Byte.SIZE * Math.ceil(this.m / (Byte.SIZE * 1.0)));
    }

    /**
     * Create bloom filter by error rate and mapping num.
     */
    public static BloomFilter createByFn(int f, int n) {
        return new BloomFilter(f, n);
    }

    /**
     * Calculate bit positions of {@code str}.
     */
    public int[] calcBitPositions(String str) {
        int[] bitPositions = new int[this.k];
        long hash64 = Hashing.murmur3_128().hashString(str, UTF_8).asLong();
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);
        for (int i = 1; i <= this.k; i++) {
            int combinedHash = hash1 + (i * hash2);
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            bitPositions[i - 1] = combinedHash % this.m;
        }
        return bitPositions;
    }

    /**
     * Calculate bit positions of {@code str} to construct {@code BloomFilterData}
     */
    public BloomFilterData generate(String str) {
        int[] bitPositions = calcBitPositions(str);
        return new BloomFilterData(bitPositions, this.m);
    }

    /**
     * Set the related {@code bits} positions to 1.
     */
    public void hashTo(int[] bitPositions, BitsArray bits) {
        check(bits);
        for (int i : bitPositions) {
            bits.setBit(i, true);
        }
    }

    /**
     * Extra check:
     * <li>1. check {@code filterData} belong to this bloom filter.</li>
     * <p>
     * Then set the related {@code bits} positions to 1.
     * </p>
     */
    public void hashTo(BloomFilterData filterData, BitsArray bits) {
        if (!isValid(filterData)) {
            throw new IllegalArgumentException(String.format("Bloom filter data may not belong to this filter! %s, %s", filterData, this));
        }
        hashTo(filterData.getBitPos(), bits);
    }

    /**
     * Check all the related {@code bits} positions is 1.
     *
     * @return true: all the related {@code bits} positions is 1
     */
    public boolean isHit(int[] bitPositions, BitsArray bits) {
        check(bits);
        boolean ret = bits.getBit(bitPositions[0]);
        for (int i = 1; i < bitPositions.length; i++) {
            ret &= bits.getBit(bitPositions[i]);
        }
        return ret;
    }

    /**
     * Check all the related {@code bits} positions is 1.
     *
     * @return true: all the related {@code bits} positions is 1
     */
    public boolean isHit(BloomFilterData filterData, BitsArray bits) {
        if (!isValid(filterData)) {
            throw new IllegalArgumentException(String.format("Bloom filter data may not belong to this filter! %s, %s", filterData, this));
        }
        return isHit(filterData.getBitPos(), bits);
    }

    protected void check(BitsArray bits) {
        if (bits.bitLength() != this.m) {
            throw new IllegalArgumentException(String.format("Length(%d) of bits in BitsArray is not equal to %d!", bits.bitLength(), this.m));
        }
    }

    /**
     * Check {@code BloomFilterData} is valid, and belong to this bloom filter.
     * <li>1. not null</li>
     * <li>2. {@link org.apache.rocketmq.filter.util.BloomFilterData#getBitNum} must be equal to {@code m} </li>
     * <li>3. {@link org.apache.rocketmq.filter.util.BloomFilterData#getBitPos} is not null</li>
     * <li>4. {@link org.apache.rocketmq.filter.util.BloomFilterData#getBitPos}'s length is equal to {@code k}</li>
     */
    public boolean isValid(BloomFilterData filterData) {
        return filterData != null && filterData.getBitNum() == this.m && filterData.getBitPos() != null && filterData.getBitPos().length == this.k;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof BloomFilter))
            return false;
        BloomFilter that = (BloomFilter) o;
        if (f != that.f)
            return false;
        if (k != that.k)
            return false;
        if (m != that.m)
            return false;
        return n == that.n;
    }

    @Override
    public int hashCode() {
        int result = f;
        result = 31 * result + n;
        result = 31 * result + k;
        result = 31 * result + m;
        return result;
    }

    @Override
    public String toString() {
        return String.format("f: %d, n: %d, k: %d, m: %d", f, n, k, m);
    }

    protected double logMN(double m, double n) {
        return Math.log(n) / Math.log(m);
    }

}