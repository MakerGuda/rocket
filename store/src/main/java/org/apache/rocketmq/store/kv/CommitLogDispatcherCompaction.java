package org.apache.rocketmq.store.kv;

import org.apache.rocketmq.store.CommitLogDispatcher;
import org.apache.rocketmq.store.DispatchRequest;

public class CommitLogDispatcherCompaction implements CommitLogDispatcher {

    private final CompactionService cptService;

    public CommitLogDispatcherCompaction(CompactionService srv) {
        this.cptService = srv;
    }

    @Override
    public void dispatch(DispatchRequest request) {
        if (cptService != null) {
            cptService.putRequest(request);
        }
    }

}