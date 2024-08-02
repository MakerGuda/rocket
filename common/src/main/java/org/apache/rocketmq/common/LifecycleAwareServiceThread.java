package org.apache.rocketmq.common;

import java.util.concurrent.TimeUnit;
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

    public void awaitStarted(long timeout) throws InterruptedException {
        long expire = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
        synchronized (started) {
            while (!started.get()) {
                long duration = expire - System.nanoTime();
                if (duration < TimeUnit.MILLISECONDS.toNanos(1)) {
                    break;
                }
                started.wait(TimeUnit.NANOSECONDS.toMillis(duration));
            }
        }
    }

}