package org.apache.rocketmq.remoting.netty;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class NettyServerConfig implements Cloneable {

    /**
     * 绑定地址
     */
    private String bindAddress = "0.0.0.0";

    /**
     * 监听端口
     */
    private int listenPort = 0;

    /**
     * netty业务线程池线程个数
     */
    private int serverWorkerThreads = 8;

    /**
     * netty public线程池线程个数
     */
    private int serverCallbackExecutorThreads = 0;

    /**
     * IO线程池线程个数
     */
    private int serverSelectorThreads = 3;

    /**
     * send oneway消息请求并发度
     */
    private int serverOnewaySemaphoreValue = 256;

    /**
     * 异步消息发送最大并发度
     */
    private int serverAsyncSemaphoreValue = 64;

    /**
     * 网络连接最大空闲时间，单位 秒
     */
    private int serverChannelMaxIdleTimeSeconds = 120;

    /**
     * 网络socket发送缓冲区大小， 默认64K
     */
    private int serverSocketSndBufSize = NettySystemConfig.socketSndbufSize;

    /**
     * 网络socket接收缓冲区大小，默认64K
     */
    private int serverSocketRcvBufSize = NettySystemConfig.socketRcvbufSize;

    /**
     * 写缓冲区高水位
     */
    private int writeBufferHighWaterMark = NettySystemConfig.writeBufferHighWaterMark;

    /**
     * 写缓冲区低水位
     */
    private int writeBufferLowWaterMark = NettySystemConfig.writeBufferLowWaterMark;

    private int serverSocketBacklog = NettySystemConfig.socketBacklog;

    /**
     * ByteBuffer是否开启缓存
     */
    private boolean serverPooledByteBufAllocatorEnable = true;

    /**
     * 是否开启优雅停机
     */
    private boolean enableShutdownGracefully = false;

    /**
     * 停机超时等待时间
     */
    private int shutdownWaitTimeSeconds = 30;

    /**
     * 是否启用epoll io模型
     */
    private boolean useEpollNativeSelector = false;


    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

}