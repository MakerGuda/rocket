package org.apache.rocketmq.broker.longpolling;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class ManyPullRequest {

    private final ArrayList<PullRequest> pullRequestList = new ArrayList<>();

    public synchronized void addPullRequest(final PullRequest pullRequest) {
        this.pullRequestList.add(pullRequest);
    }

    public synchronized void addPullRequest(final List<PullRequest> many) {
        this.pullRequestList.addAll(many);
    }

    public synchronized List<PullRequest> cloneListAndClear() {
        if (!this.pullRequestList.isEmpty()) {
            List<PullRequest> result = (ArrayList<PullRequest>) this.pullRequestList.clone();
            this.pullRequestList.clear();
            return result;
        }

        return null;
    }

    public synchronized boolean isEmpty() {
        return this.pullRequestList.isEmpty();
    }

}