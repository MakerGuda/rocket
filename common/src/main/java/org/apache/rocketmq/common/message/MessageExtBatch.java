package org.apache.rocketmq.common.message;

import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;

@Getter
@Setter
public class MessageExtBatch extends MessageExtBrokerInner {

    private static final long serialVersionUID = -2353110995348498537L;

    private boolean isInnerBatch = false;

    private ByteBuffer encodedBuff;

    public ByteBuffer wrap() {
        assert getBody() != null;
        return ByteBuffer.wrap(getBody(), 0, getBody().length);
    }

    public boolean isInnerBatch() {
        return isInnerBatch;
    }

    public void setInnerBatch(boolean innerBatch) {
        isInnerBatch = innerBatch;
    }

}