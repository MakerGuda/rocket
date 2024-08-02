package org.apache.rocketmq.common.future;

import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.FutureTask;

@Getter
@Setter
public class FutureTaskExt<V> extends FutureTask<V> {

    private final Runnable runnable;

    public FutureTaskExt(final Runnable runnable, final V result) {
        super(runnable, result);
        this.runnable = runnable;
    }

}