package org.apache.rocketmq.tools.command;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.tools.admin.MQAdminExt;

import java.util.*;
import java.util.Map.Entry;

public class CommandUtil {

    public static final String NO_MASTER_PLACEHOLDER = "NO_MASTER";

    private static final String ERROR_MESSAGE = "Make sure the specified clusterName exists or the name server connected to is correct.";

    public static Map<String, List<String>> fetchMasterAndSlaveDistinguish(final MQAdminExt adminExt, final String clusterName) throws InterruptedException, RemotingConnectException, RemotingTimeoutException, RemotingSendRequestException, MQBrokerException {
        Map<String, List<String>> masterAndSlaveMap = new HashMap<>(4);
        ClusterInfo clusterInfoSerializeWrapper = adminExt.examineBrokerClusterInfo();
        Set<String> brokerNameSet = clusterInfoSerializeWrapper.getClusterAddrTable().get(clusterName);
        if (brokerNameSet == null) {
            System.out.printf("[error] %s", ERROR_MESSAGE);
            return masterAndSlaveMap;
        }
        for (String brokerName : brokerNameSet) {
            BrokerData brokerData = clusterInfoSerializeWrapper.getBrokerAddrTable().get(brokerName);
            if (brokerData == null || brokerData.getBrokerAddrs() == null) {
                continue;
            }
            String masterAddr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
            if (masterAddr == null) {
                masterAndSlaveMap.putIfAbsent(NO_MASTER_PLACEHOLDER, new ArrayList<>());
            } else {
                masterAndSlaveMap.put(masterAddr, new ArrayList<>());
            }
            for (Entry<Long, String> brokerAddrEntry : brokerData.getBrokerAddrs().entrySet()) {
                if (brokerAddrEntry.getValue() == null || brokerAddrEntry.getKey() == MixAll.MASTER_ID) {
                    continue;
                }
                if (masterAddr == null) {
                    masterAndSlaveMap.get(NO_MASTER_PLACEHOLDER).add(brokerAddrEntry.getValue());
                } else {
                    masterAndSlaveMap.get(masterAddr).add(brokerAddrEntry.getValue());
                }
            }
        }
        return masterAndSlaveMap;
    }

    public static Set<String> fetchMasterAddrByClusterName(final MQAdminExt adminExt, final String clusterName) throws InterruptedException, RemotingConnectException, RemotingTimeoutException, RemotingSendRequestException, MQBrokerException {
        Set<String> masterSet = new HashSet<>();
        ClusterInfo clusterInfoSerializeWrapper = adminExt.examineBrokerClusterInfo();
        Set<String> brokerNameSet = clusterInfoSerializeWrapper.getClusterAddrTable().get(clusterName);
        if (brokerNameSet != null) {
            for (String brokerName : brokerNameSet) {
                BrokerData brokerData = clusterInfoSerializeWrapper.getBrokerAddrTable().get(brokerName);
                if (brokerData != null) {
                    String addr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
                    if (addr != null) {
                        masterSet.add(addr);
                    }
                }
            }
        } else {
            System.out.printf("[error] %s", ERROR_MESSAGE);
        }
        return masterSet;
    }

    public static Set<String> fetchMasterAndSlaveAddrByClusterName(final MQAdminExt adminExt, final String clusterName) throws InterruptedException, RemotingConnectException, RemotingTimeoutException, RemotingSendRequestException, MQBrokerException {
        Set<String> brokerAddressSet = new HashSet<>();
        ClusterInfo clusterInfoSerializeWrapper = adminExt.examineBrokerClusterInfo();
        Set<String> brokerNameSet = clusterInfoSerializeWrapper.getClusterAddrTable().get(clusterName);
        if (brokerNameSet != null) {
            for (String brokerName : brokerNameSet) {
                BrokerData brokerData = clusterInfoSerializeWrapper.getBrokerAddrTable().get(brokerName);
                if (brokerData != null) {
                    final Collection<String> addrs = brokerData.getBrokerAddrs().values();
                    brokerAddressSet.addAll(addrs);
                }
            }
        } else {
            System.out.printf("[error] %s", ERROR_MESSAGE);
        }
        return brokerAddressSet;
    }

    public static Set<String> fetchMasterAndSlaveAddrByBrokerName(final MQAdminExt adminExt, final String brokerName) throws InterruptedException, RemotingConnectException, RemotingTimeoutException, RemotingSendRequestException, MQBrokerException {
        Set<String> brokerAddressSet = new HashSet<>();
        ClusterInfo clusterInfoSerializeWrapper = adminExt.examineBrokerClusterInfo();
        final BrokerData brokerData = clusterInfoSerializeWrapper.getBrokerAddrTable().get(brokerName);
        if (brokerData != null) {
            brokerAddressSet.addAll(brokerData.getBrokerAddrs().values());
        }
        return brokerAddressSet;
    }

    public static Set<String> fetchBrokerNameByClusterName(final MQAdminExt adminExt, final String clusterName) throws Exception {
        ClusterInfo clusterInfoSerializeWrapper = adminExt.examineBrokerClusterInfo();
        Set<String> brokerNameSet = clusterInfoSerializeWrapper.getClusterAddrTable().get(clusterName);
        if (brokerNameSet == null || brokerNameSet.isEmpty()) {
            throw new Exception(ERROR_MESSAGE);
        }
        return brokerNameSet;
    }

    public static String fetchBrokerNameByAddr(final MQAdminExt adminExt, final String addr) throws Exception {
        ClusterInfo clusterInfoSerializeWrapper = adminExt.examineBrokerClusterInfo();
        Map<String, BrokerData> brokerAddrTable = clusterInfoSerializeWrapper.getBrokerAddrTable();
        for (Entry<String, BrokerData> entry : brokerAddrTable.entrySet()) {
            HashMap<Long, String> brokerAddrs = entry.getValue().getBrokerAddrs();
            if (brokerAddrs.containsValue(addr)) {
                return entry.getKey();
            }
        }
        throw new Exception(ERROR_MESSAGE);
    }

}