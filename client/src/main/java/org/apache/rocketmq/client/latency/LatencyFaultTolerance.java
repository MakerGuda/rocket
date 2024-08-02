package org.apache.rocketmq.client.latency;

public interface LatencyFaultTolerance<T> {

    /**
     * 更新broker的状态，用来判断他们是否可用
     *
     * @param name                 brokerName
     * @param currentLatency       Current message sending process's latency.
     * @param notAvailableDuration 不可用时间，单位为毫秒，时间范围内broker不可用
     * @param reachable            判断当前broker是否可达
     */
    void updateFaultItem(final T name, final long currentLatency, final long notAvailableDuration, final boolean reachable);

    /**
     * 判断当前broker是否可用
     *
     * @param name brokerName
     */
    boolean isAvailable(final T name);

    /**
     * 判断当前broker是否可达
     *
     * @param name brokerName
     */
    boolean isReachable(final T name);

    /**
     * 将broker从容错表中移除
     *
     * @param name brokerName
     */
    void remove(final T name);

    /**
     * 启动一个线程，嗅探broker是否可达
     */
    void startDetector();

    /**
     * 关闭线程
     */
    void shutdown();

    /**
     * 简单嗅探一轮，不创建新线程
     */
    void detectByOneRound();

    /**
     * 设置嗅探超时时间
     */
    void setDetectTimeout(final int detectTimeout);

    /**
     * 设置嗅探时间间隔
     */
    void setDetectInterval(final int detectInterval);

    /**
     * 设置嗅探工作状态
     */
    void setStartDetectorEnable(final boolean startDetectorEnable);

}