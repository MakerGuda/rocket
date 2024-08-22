package org.apache.rocketmq.tools.monitor;

import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;

import java.util.TreeMap;

public interface MonitorListener {

    void beginRound();

    void reportUndoneMsgs(UndoneMsgs undoneMsgs);

    void reportDeleteMsgsEvent(DeleteMsgsEvent deleteMsgsEvent);

    void reportConsumerRunningInfo(TreeMap<String, ConsumerRunningInfo> criTable);

    void endRound();

}