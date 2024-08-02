package org.apache.rocketmq.namesrv.routeinfo;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.header.namesrv.UnRegisterBrokerRequestHeader;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 批量broker服务下线服务
 */
public class BatchUnregistrationService extends ServiceThread {

    private final RouteInfoManager routeInfoManager;

    /**
     * 阻塞队列
     */
    private final BlockingQueue<UnRegisterBrokerRequestHeader> unregistrationQueue;

    private static final Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    public BatchUnregistrationService(RouteInfoManager routeInfoManager, NamesrvConfig namesrvConfig) {
        this.routeInfoManager = routeInfoManager;
        this.unregistrationQueue = new LinkedBlockingQueue<>(namesrvConfig.getUnRegisterBrokerQueueCapacity());
    }

    /**
     * 提交注销broker请求，添加到队列中
     */
    public boolean submit(UnRegisterBrokerRequestHeader unRegisterRequest) {
        return unregistrationQueue.offer(unRegisterRequest);
    }

    @Override
    public String getServiceName() {
        return BatchUnregistrationService.class.getName();
    }

    @Override
    public void run() {
        while (!this.isStopped()) {
            try {
                final UnRegisterBrokerRequestHeader request = unregistrationQueue.take();
                Set<UnRegisterBrokerRequestHeader> unregistrationRequests = new HashSet<>();
                unregistrationQueue.drainTo(unregistrationRequests);
                unregistrationRequests.add(request);
                this.routeInfoManager.unRegisterBroker(unregistrationRequests);
            } catch (Throwable e) {
                log.error("Handle unregister broker request failed", e);
            }
        }
    }

}