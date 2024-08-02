package org.apache.rocketmq.common.namesrv;


public interface TopAddressing {

    String fetchNSAddr();

    void registerChangeCallBack(NameServerUpdateCallback changeCallBack);

}