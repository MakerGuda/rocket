package org.apache.rocketmq.common;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class LifecycleAwareServiceThread extends ServiceThread {

    private final AtomicBoolean started = new AtomicBoolean(false);

    @Override
    public void run() {
        started.set(true);
        synchronized (started) {
            started.notifyAll();
        }
        run0();
    }

    public abstract void run0();

}