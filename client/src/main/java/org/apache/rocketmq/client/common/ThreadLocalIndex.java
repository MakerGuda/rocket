package org.apache.rocketmq.client.common;

import java.util.Random;


public class ThreadLocalIndex {

    private final ThreadLocal<Integer> threadLocalIndex = new ThreadLocal<>();

    private final Random random = new Random();

    private final static int POSITIVE_MASK = 0x7FFFFFFF;

    public int incrementAndGet() {
        Integer index = this.threadLocalIndex.get();
        if (null == index) {
            index = random.nextInt();
        }
        this.threadLocalIndex.set(++index);
        return index & POSITIVE_MASK;
    }

    /**
     * 重置threadLocal索引
     */
    public void reset() {
        int index = Math.abs(random.nextInt(Integer.MAX_VALUE));
        this.threadLocalIndex.set(index);
    }

    @Override
    public String toString() {
        return "ThreadLocalIndex{" + "threadLocalIndex=" + threadLocalIndex.get() + '}';
    }

}