package org.apache.rocketmq.remoting.rpc;

public abstract class TopicRequestHeader extends RpcRequestHeader {

    protected Boolean lo;

    public abstract String getTopic();

    public abstract void setTopic(String topic);

    public Boolean getLo() {
        return lo;
    }

    public void setLo(Boolean lo) {
        this.lo = lo;
    }

}