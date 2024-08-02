package org.apache.rocketmq.common.utils;

public interface StartAndShutdown extends Start, Shutdown {

    default void preShutdown() throws Exception {}

}