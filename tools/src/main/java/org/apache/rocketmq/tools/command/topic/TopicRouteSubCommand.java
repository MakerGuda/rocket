package org.apache.rocketmq.tools.command.topic;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopicRouteSubCommand implements SubCommand {

    private static final String FORMAT = "%-45s %-32s %-50s %-10s %-11s %-5s%n";

    @Override
    public String commandName() {
        return "topicRoute";
    }

    @Override
    public String commandDesc() {
        return "Examine topic route info.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option("l", "list", false, "Use list format to print data");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(final CommandLine commandLine, final Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            defaultMQAdminExt.start();
            String topic = commandLine.getOptionValue('t').trim();
            TopicRouteData topicRouteData = defaultMQAdminExt.examineTopicRouteInfo(topic);
            printData(topicRouteData, commandLine.hasOption('l'));
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    private void printData(TopicRouteData topicRouteData, boolean useListFormat) {
        if (!useListFormat) {
            System.out.printf("%s%n", topicRouteData.toJson(true));
            return;
        }
        int totalReadQueue = 0, totalWriteQueue = 0;
        List<QueueData> queueDataList = topicRouteData.getQueueDatas();
        Map<String, QueueData> map = new HashMap<>();
        for (QueueData queueData : queueDataList) {
            map.put(queueData.getBrokerName(), queueData);
        }
        queueDataList.sort(Comparator.comparing(QueueData::getBrokerName));
        List<BrokerData> brokerDataList = topicRouteData.getBrokerDatas();
        brokerDataList.sort(Comparator.comparing(BrokerData::getBrokerName));
        System.out.printf(FORMAT, "#ClusterName", "#BrokerName", "#BrokerAddrs", "#ReadQueue", "#WriteQueue", "#Perm");
        for (BrokerData brokerData : brokerDataList) {
            String brokerName = brokerData.getBrokerName();
            QueueData queueData = map.get(brokerName);
            totalReadQueue += queueData.getReadQueueNums();
            totalWriteQueue += queueData.getWriteQueueNums();
            System.out.printf(FORMAT, brokerData.getCluster(), brokerName, brokerData.getBrokerAddrs(), queueData.getReadQueueNums(), queueData.getWriteQueueNums(), queueData.getPerm());
        }
        for (int i = 0; i < 158; i++) {
            System.out.print("-");
        }
        System.out.printf("%n");
        System.out.printf(FORMAT, "Total:", map.keySet().size(), "", totalReadQueue, totalWriteQueue, "");
    }

}