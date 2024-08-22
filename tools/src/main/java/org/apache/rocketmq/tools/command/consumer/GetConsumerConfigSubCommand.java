package org.apache.rocketmq.tools.command.consumer;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class GetConsumerConfigSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "getConsumerConfig";
    }

    @Override
    public String commandDesc() {
        return "Get consumer config by subscription group name.";
    }

    @Override
    public Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("g", "groupName", true, "subscription group name");
        opt.setRequired(true);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt adminExt = new DefaultMQAdminExt(rpcHook);
        adminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        String groupName = commandLine.getOptionValue('g').trim();
        if (commandLine.hasOption('n')) {
            adminExt.setNamesrvAddr(commandLine.getOptionValue('n').trim());
        }
        try {
            adminExt.start();
            List<ConsumerConfigInfo> consumerConfigInfoList = new ArrayList<>();
            ClusterInfo clusterInfo = adminExt.examineBrokerClusterInfo();
            Map<String, Set<String>> clusterAddrTable = clusterInfo.getClusterAddrTable();
            for (Entry<String, BrokerData> brokerEntry : clusterInfo.getBrokerAddrTable().entrySet()) {
                String clusterName = this.getClusterName(brokerEntry.getKey(), clusterAddrTable);
                String brokerAddress = brokerEntry.getValue().selectBrokerAddr();
                SubscriptionGroupConfig subscriptionGroupConfig = adminExt.examineSubscriptionGroupConfig(brokerAddress, groupName);
                if (subscriptionGroupConfig == null) {
                    continue;
                }
                consumerConfigInfoList.add(new ConsumerConfigInfo(clusterName, brokerEntry.getKey(), subscriptionGroupConfig));
            }
            if (CollectionUtils.isEmpty(consumerConfigInfoList)) {
                return;
            }
            for (ConsumerConfigInfo info : consumerConfigInfoList) {
                System.out.printf("=============================%s:%s=============================\n", info.getClusterName(), info.getBrokerName());
                SubscriptionGroupConfig config = info.getSubscriptionGroupConfig();
                Field[] fields = config.getClass().getDeclaredFields();
                for (Field field : fields) {
                    field.setAccessible(true);
                    if (field.get(config) != null) {
                        System.out.printf("%s%-40s=  %s\n", "", field.getName(), field.get(config).toString());
                    } else {
                        System.out.printf("%s%-40s=  %s\n", "", field.getName(), "");
                    }
                }
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            adminExt.shutdown();
        }
    }

    private String getClusterName(String brokeName, Map<String, Set<String>> clusterAddrTable) {
        for (Map.Entry<String, Set<String>> entry : clusterAddrTable.entrySet()) {
            Set<String> brokerNameSet = entry.getValue();
            if (brokerNameSet.contains(brokeName)) {
                return entry.getKey();
            }
        }
        return null;
    }
}

@Getter
@Setter
class ConsumerConfigInfo {

    private String clusterName;

    private String brokerName;

    private SubscriptionGroupConfig subscriptionGroupConfig;

    public ConsumerConfigInfo(String clusterName, String brokerName, SubscriptionGroupConfig subscriptionGroupConfig) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.subscriptionGroupConfig = subscriptionGroupConfig;
    }

}