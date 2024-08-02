package org.apache.rocketmq.common;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class JraftConfig {

    private int jRaftElectionTimeoutMs = 1000;

    private int jRaftScanWaitTimeoutMs = 1000;

    private int jRaftSnapshotIntervalSecs = 3600;

    private String jRaftGroupId = "jRaft-Controller";

    private String jRaftServerId = "localhost:9880";

    private String jRaftInitConf = "localhost:9880,localhost:9881,localhost:9882";

    private String jRaftControllerRPCAddr = "localhost:9770,localhost:9771,localhost:9772";

}