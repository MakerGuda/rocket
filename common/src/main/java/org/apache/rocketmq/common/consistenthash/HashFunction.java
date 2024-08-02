package org.apache.rocketmq.common.consistenthash;

public interface HashFunction {

    long hash(String key);

}