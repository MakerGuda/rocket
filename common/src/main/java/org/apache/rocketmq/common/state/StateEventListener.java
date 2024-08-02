package org.apache.rocketmq.common.state;

public interface StateEventListener<T> {

    void fireEvent(T event);

}