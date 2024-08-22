package org.apache.rocketmq.example.simple;

import lombok.Getter;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.TreeMap;

@Getter
public class CachedQueue {

    private final TreeMap<Long, MessageExt> msgCachedTable = new TreeMap<>();

}