package org.apache.rocketmq.client.latency;

/**
 * 嗅探远程服务状态是否正常
 */
public interface ServiceDetector {

    /**
     * 检查远程服务状态是否正常
     *
     * @param endpoint      远程服务端点
     * @param timeoutMillis 超时时间
     * @return 返回true表示服务正常
     */
    boolean detect(String endpoint, long timeoutMillis);

}