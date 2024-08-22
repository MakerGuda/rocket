package org.apache.rocketmq.common.namesrv;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.MixAll;

import java.io.File;

@Getter
@Setter
public class NamesrvConfig {

    /**
     * rocketmq根目录文件
     */
    private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));

    /**
     * k-v 配置文件
     */
    private String kvConfigPath = System.getProperty("user.home") + File.separator + "namesrv" + File.separator + "kvConfig.json";

    /**
     * namesrv配置文件
     */
    private String configStorePath = System.getProperty("user.home") + File.separator + "namesrv" + File.separator + "namesrv.properties";

    private String productEnvName = "center";

    private boolean clusterTest = false;

    /**
     * 是否支持顺序消息，默认不支持
     */
    private boolean orderMessageEnable = false;

    private boolean returnOrderTopicConfigToBroker = true;

    /**
     * 处理客户端请求的线程数
     */
    private int clientRequestThreadPoolNums = 8;

    /**
     * 处理broker或者操作请求的线程数
     */
    private int defaultThreadPoolNums = 16;

    /**
     * 存放客户端请求的队列大小
     */
    private int clientRequestThreadPoolQueueCapacity = 50000;

    /**
     * 处理broker或操作请求的队列大小
     */
    private int defaultThreadPoolQueueCapacity = 10000;

    /**
     * 扫描非活跃状态broker的间隔时间，默认为5秒
     */
    private long scanNotActiveBrokerInterval = 5 * 1000;

    /**
     * 存放注销broker请求的队列大小
     */
    private int unRegisterBrokerQueueCapacity = 3000;

    /**
     * 是否支持激活master
     */
    private boolean supportActingMaster = false;

    /**
     * 是否允许获取所有主题列表
     */
    private volatile boolean enableAllTopicList = true;

    /**
     * 是否允许获取主题列表
     */
    private volatile boolean enableTopicList = true;

    /**
     * 当最小brokerId发生改变时，是否发送通知
     */
    private volatile boolean notifyMinBrokerIdChanged = false;

    /**
     * 是否在namesrv中启动controller
     */
    private boolean enableControllerInNamesrv = false;

    private volatile boolean needWaitForService = false;

    private int waitSecondsForService = 45;

    /**
     * If enable this flag, the topics that don't exist in broker registration payload will be deleted from name server.
     * 1. Enable this flag and "enableSingleTopicRegister" of broker config meanwhile to avoid losing topic route info unexpectedly.
     * 2. This flag does not support static topic currently.
     */
    private boolean deleteTopicWithBrokerRegistration = false;

    /**
     * 配置黑名单，将不允许被命令更新，比如重启进程
     */
    private String configBlackList = "configBlackList;configStorePath;kvConfigPath";

}