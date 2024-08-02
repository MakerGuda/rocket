package org.apache.rocketmq.common;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.metrics.MetricsExporterType;

import java.io.File;
import java.util.Arrays;

@Getter
@Setter
public class ControllerConfig {

    public static final String DLEDGER_CONTROLLER = "DLedger";
    public static final String JRAFT_CONTROLLER = "jRaft";
    private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));
    private String configStorePath = System.getProperty("user.home") + File.separator + "controller" + File.separator + "controller.properties";
    private JraftConfig jraftConfig = new JraftConfig();

    private String controllerType = DLEDGER_CONTROLLER;

    private long scanNotActiveBrokerInterval = 5 * 1000;

    private int controllerThreadPoolNums = 16;

    private int controllerRequestThreadPoolQueueCapacity = 50000;

    private String controllerDLegerGroup;

    private String controllerDLegerPeers;

    private String controllerDLegerSelfId;

    private int mappedFileSize = 1024 * 1024 * 1024;

    private String controllerStorePath = "";

    private int electMasterMaxRetryCount = 3;

    private boolean enableElectUncleanMaster = false;

    private boolean isProcessReadEvent = false;

    private volatile boolean notifyBrokerRoleChanged = true;

    private long scanInactiveMasterInterval = 5 * 1000;

    private MetricsExporterType metricsExporterType = MetricsExporterType.DISABLE;

    private String metricsGrpcExporterTarget = "";

    private String metricsGrpcExporterHeader = "";

    private long metricGrpcExporterTimeOutInMills = 3 * 1000;

    private long metricGrpcExporterIntervalInMills = 60 * 1000;

    private long metricLoggingExporterIntervalInMills = 10 * 1000;

    private int metricsPromExporterPort = 5557;

    private String metricsPromExporterHost = "";

    private String metricsLabel = "";

    private boolean metricsInDelta = false;

    private String configBlackList = "configBlackList;configStorePath";

    public String getControllerStorePath() {
        if (controllerStorePath.isEmpty()) {
            controllerStorePath = System.getProperty("user.home") + File.separator + controllerType + "Controller";
        }
        return controllerStorePath;
    }

    public String getDLedgerAddress() {
        return Arrays.stream(this.controllerDLegerPeers.split(";")).filter(x -> this.controllerDLegerSelfId.equals(x.split("-")[0])).map(x -> x.split("-")[1]).findFirst().orElse("");
    }

}