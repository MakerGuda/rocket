package org.apache.rocketmq.common;

public enum ServiceState {

    /**
     * 创建未启动
     */
    CREATE_JUST,
    /**
     * 服务运行中
     */
    RUNNING,
    /**
     * 服务关闭
     */
    SHUTDOWN_ALREADY,
    /**
     * 服务启动失败
     */
    START_FAILED

}