package org.apache.rocketmq.broker.client;

public interface ProducerChangeListener {

    void handle(ProducerGroupEvent event, String group, ClientChannelInfo clientChannelInfo);

}