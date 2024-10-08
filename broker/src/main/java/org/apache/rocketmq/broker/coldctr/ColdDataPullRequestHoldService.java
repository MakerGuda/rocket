package org.apache.rocketmq.broker.coldctr;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.longpolling.PullRequest;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

@Getter
@Setter
public class ColdDataPullRequestHoldService extends ServiceThread {

    public static final String NO_SUSPEND_KEY = "_noSuspend_";

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_COLDCTR_LOGGER_NAME);

    private final long coldHoldTimeoutMillis = 3000;

    private final SystemClock systemClock = new SystemClock();

    private final BrokerController brokerController;

    private final LinkedBlockingQueue<PullRequest> pullRequestColdHoldQueue = new LinkedBlockingQueue<>(10000);

    public ColdDataPullRequestHoldService(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void suspendColdDataReadRequest(PullRequest pullRequest) {
        if (this.brokerController.getMessageStoreConfig().isColdDataFlowControlEnable()) {
            pullRequestColdHoldQueue.offer(pullRequest);
        }
    }

    @Override
    public String getServiceName() {
        return ColdDataPullRequestHoldService.class.getSimpleName();
    }

    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        while (!this.isStopped()) {
            try {
                if (!this.brokerController.getMessageStoreConfig().isColdDataFlowControlEnable()) {
                    this.waitForRunning(20 * 1000);
                } else {
                    this.waitForRunning(5 * 1000);
                }
                long beginClockTimestamp = this.systemClock.now();
                this.checkColdDataPullRequest();
                long costTime = this.systemClock.now() - beginClockTimestamp;
                log.info("[{}] checkColdDataPullRequest-cost {} ms.", costTime > 5 * 1000 ? "NOTIFYME" : "OK", costTime);
            } catch (Throwable e) {
                log.warn(this.getServiceName() + " service has exception", e);
            }
        }
        log.info("{} service end", this.getServiceName());
    }

    private void checkColdDataPullRequest() {
        int succTotal = 0, errorTotal = 0, queueSize = pullRequestColdHoldQueue.size();
        Iterator<PullRequest> iterator = pullRequestColdHoldQueue.iterator();
        while (iterator.hasNext()) {
            PullRequest pullRequest = iterator.next();
            if (System.currentTimeMillis() >= pullRequest.getSuspendTimestamp() + coldHoldTimeoutMillis) {
                try {
                    pullRequest.getRequestCommand().addExtField(NO_SUSPEND_KEY, "1");
                    this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(pullRequest.getClientChannel(), pullRequest.getRequestCommand());
                    succTotal++;
                } catch (Exception e) {
                    log.error("PullRequestColdHoldService checkColdDataPullRequest error", e);
                    errorTotal++;
                }
                iterator.remove();
            }
        }
        log.info("checkColdPullRequest-info-finish, queueSize: {} successTotal: {} errorTotal: {}", queueSize, succTotal, errorTotal);
    }

}