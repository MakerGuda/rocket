package org.apache.rocketmq.store;

import lombok.Getter;

import java.util.concurrent.atomic.AtomicLong;

public abstract class ReferenceResource {

    protected final AtomicLong refCount = new AtomicLong(1);

    @Getter
    protected volatile boolean available = true;

    protected volatile boolean cleanupOver = false;

    private volatile long firstShutdownTimestamp = 0;

    public synchronized boolean hold() {
        if (this.isAvailable()) {
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                this.refCount.getAndDecrement();
            }
        }
        return false;
    }

    /**
     * 关闭
     */
    public void shutdown(final long intervalForcibly) {
        if (this.available) {
            this.available = false;
            //首次销毁时间
            this.firstShutdownTimestamp = System.currentTimeMillis();
            //release()方法只有在引用次数小于1的情况下才会释放资源
            this.release();
        } else if (this.getRefCount() > 0) {
            //如果引用次数大于0，判断存活时间是否超过最大拒绝存活时间，则每次执行时将引用次数-1000，直到减为0时将文件销毁
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                this.refCount.set(-1000 - this.getRefCount());
                this.release();
            }
        }
    }

    /**
     * 释放
     */
    public void release() {
        long value = this.refCount.decrementAndGet();
        if (value > 0)
            return;
        synchronized (this) {
            this.cleanupOver = this.cleanup(value);
        }
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    public abstract boolean cleanup(final long currentRef);

    /**
     * 判断是否清理完成，引用次数小于等于0，并且cleanupOver等于true时表示清理完成
     */
    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }

}