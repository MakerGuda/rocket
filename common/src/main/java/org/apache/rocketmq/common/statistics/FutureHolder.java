package org.apache.rocketmq.common.statistics;

import java.util.concurrent.*;

public class FutureHolder<T> {

    private final ConcurrentMap<T, BlockingQueue<Future>> futureMap = new ConcurrentHashMap<>(8);

    public void addFuture(T t, Future future) {
        BlockingQueue<Future> list = futureMap.get(t);
        if (list == null) {
            list = new LinkedBlockingQueue<>();
            BlockingQueue<Future> old = futureMap.putIfAbsent(t, list);
            if (old != null) {
                list = old;
            }
        }
        list.add(future);
    }

    public void removeAllFuture(T t) {
        cancelAll(t);
        futureMap.remove(t);
    }

    private void cancelAll(T t) {
        BlockingQueue<Future> list = futureMap.get(t);
        if (list != null) {
            for (Future future : list) {
                future.cancel(false);
            }
        }
    }

}