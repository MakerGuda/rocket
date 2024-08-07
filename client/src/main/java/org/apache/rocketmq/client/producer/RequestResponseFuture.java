package org.apache.rocketmq.client.producer;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Getter
@Setter
public class RequestResponseFuture {

    private final String correlationId;

    private final RequestCallback requestCallback;

    private final long beginTimestamp = System.currentTimeMillis();

    private final Message requestMsg = null;

    private long timeoutMillis;

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    private volatile Message responseMsg = null;

    private volatile boolean sendRequestOk = true;

    private volatile Throwable cause = null;

    public RequestResponseFuture(String correlationId, long timeoutMillis, RequestCallback requestCallback) {
        this.correlationId = correlationId;
        this.timeoutMillis = timeoutMillis;
        this.requestCallback = requestCallback;
    }

    public void executeRequestCallback() {
        if (requestCallback != null) {
            if (sendRequestOk && cause == null) {
                requestCallback.onSuccess(responseMsg);
            } else {
                requestCallback.onException(cause);
            }
        }
    }

    public boolean isTimeout() {
        long diff = System.currentTimeMillis() - this.beginTimestamp;
        return diff > this.timeoutMillis;
    }

    public Message waitResponseMessage(final long timeout) throws InterruptedException {
        this.countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
        return this.responseMsg;
    }

    public void putResponseMessage(final Message responseMsg) {
        this.responseMsg = responseMsg;
        this.countDownLatch.countDown();
    }

}