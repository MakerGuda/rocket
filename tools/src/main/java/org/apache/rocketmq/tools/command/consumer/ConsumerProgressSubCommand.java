package org.apache.rocketmq.tools.command.consumer;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper;
import org.apache.rocketmq.remoting.protocol.body.Connection;
import org.apache.rocketmq.remoting.protocol.body.ConsumerConnection;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.body.TopicList;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.*;

public class ConsumerProgressSubCommand implements SubCommand {

    private static final Logger log = LoggerFactory.getLogger(ConsumerProgressSubCommand.class);

    @Override
    public String commandName() {
        return "consumerProgress";
    }

    @Override
    public String commandDesc() {
        return "Query consumer's progress, speed.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("g", "groupName", true, "consumer group name");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("t", "topicName", true, "topic name");
        opt.setRequired(false);
        options.addOption(opt);
        Option optionShowClientIP = new Option("s", "showClientIP", true, "Show Client IP per Queue");
        optionShowClientIP.setRequired(false);
        options.addOption(optionShowClientIP);
        return options;
    }

    private Map<MessageQueue, String> getMessageQueueAllocationResult(DefaultMQAdminExt defaultMQAdminExt, String groupName) {
        Map<MessageQueue, String> results = new HashMap<>();
        try {
            ConsumerConnection consumerConnection = defaultMQAdminExt.examineConsumerConnectionInfo(groupName);
            for (Connection connection : consumerConnection.getConnectionSet()) {
                String clientId = connection.getClientId();
                ConsumerRunningInfo consumerRunningInfo = defaultMQAdminExt.getConsumerRunningInfo(groupName, clientId, false, false);
                for (MessageQueue messageQueue : consumerRunningInfo.getMqTable().keySet()) {
                    results.put(messageQueue, clientId.split("@")[0]);
                }
            }
        } catch (Exception e) {
            log.error("getMqAllocationsResult error, ", e);
        }
        return results;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        if (commandLine.hasOption('n')) {
            defaultMQAdminExt.setNamesrvAddr(commandLine.getOptionValue('n').trim());
        }
        try {
            defaultMQAdminExt.start();
            boolean showClientIP = commandLine.hasOption('s') && "true".equalsIgnoreCase(commandLine.getOptionValue('s'));
            if (commandLine.hasOption('g')) {
                String consumerGroup = commandLine.getOptionValue('g').trim();
                String topicName = commandLine.hasOption('t') ? commandLine.getOptionValue('t').trim() : null;
                ConsumeStats consumeStats;
                if (topicName == null) {
                    consumeStats = defaultMQAdminExt.examineConsumeStats(consumerGroup);
                } else {
                    consumeStats = defaultMQAdminExt.examineConsumeStats(consumerGroup, topicName);
                }
                List<MessageQueue> mqList = new LinkedList<>(consumeStats.getOffsetTable().keySet());
                Collections.sort(mqList);
                Map<MessageQueue, String> messageQueueAllocationResult = null;
                if (showClientIP) {
                    messageQueueAllocationResult = getMessageQueueAllocationResult(defaultMQAdminExt, consumerGroup);
                }
                if (showClientIP) {
                    System.out.printf("%-64s  %-32s  %-4s  %-20s  %-20s  %-20s %-20s %-20s%s%n", "#Topic", "#Broker Name", "#QID", "#Broker Offset", "#Consumer Offset", "#Client IP", "#Diff", "#Inflight", "#LastTime");
                } else {
                    System.out.printf("%-64s  %-32s  %-4s  %-20s  %-20s  %-20s %-20s%s%n", "#Topic", "#Broker Name", "#QID", "#Broker Offset", "#Consumer Offset", "#Diff", "#Inflight", "#LastTime");
                }
                long diffTotal = 0L;
                long inflightTotal = 0L;
                for (MessageQueue mq : mqList) {
                    OffsetWrapper offsetWrapper = consumeStats.getOffsetTable().get(mq);
                    long diff = offsetWrapper.getBrokerOffset() - offsetWrapper.getConsumerOffset();
                    long inflight = offsetWrapper.getPullOffset() - offsetWrapper.getConsumerOffset();
                    diffTotal += diff;
                    inflightTotal += inflight;
                    String lastTime = "";
                    try {
                        if (offsetWrapper.getLastTimestamp() == 0) {
                            lastTime = "N/A";
                        } else {
                            lastTime = UtilAll.formatDate(new Date(offsetWrapper.getLastTimestamp()), UtilAll.YYYY_MM_DD_HH_MM_SS);
                        }
                    } catch (Exception ignore) {
                    }
                    String clientIP = null;
                    if (showClientIP) {
                        clientIP = messageQueueAllocationResult.get(mq);
                    }
                    if (showClientIP) {
                        System.out.printf("%-64s  %-32s  %-4d  %-20d  %-20d  %-20s %-20d %-20d %s%n", UtilAll.frontStringAtLeast(mq.getTopic(), 64), UtilAll.frontStringAtLeast(mq.getBrokerName(), 32), mq.getQueueId(), offsetWrapper.getBrokerOffset(), offsetWrapper.getConsumerOffset(), null != clientIP ? clientIP : "N/A", diff, inflight, lastTime);
                    } else {
                        System.out.printf("%-64s  %-32s  %-4d  %-20d  %-20d  %-20d %-20d %s%n", UtilAll.frontStringAtLeast(mq.getTopic(), 64), UtilAll.frontStringAtLeast(mq.getBrokerName(), 32), mq.getQueueId(), offsetWrapper.getBrokerOffset(), offsetWrapper.getConsumerOffset(), diff, inflight, lastTime);
                    }
                }
                System.out.printf("%n");
                System.out.printf("Consume TPS: %.2f%n", consumeStats.getConsumeTps());
                System.out.printf("Consume Diff Total: %d%n", diffTotal);
                System.out.printf("Consume Inflight Total: %d%n", inflightTotal);
            } else {
                System.out.printf("%-64s  %-6s  %-24s %-5s  %-14s  %-7s  %s%n", "#Group", "#Count", "#Version", "#Type", "#Model", "#TPS", "#Diff Total");
                TopicList topicList = defaultMQAdminExt.fetchAllTopicList();
                for (String topic : topicList.getTopicList()) {
                    if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        String consumerGroup = KeyBuilder.parseGroup(topic);
                        try {
                            ConsumeStats consumeStats = null;
                            try {
                                consumeStats = defaultMQAdminExt.examineConsumeStats(consumerGroup);
                            } catch (Exception e) {
                                log.warn("examineConsumeStats exception, " + consumerGroup, e);
                            }
                            ConsumerConnection cc = null;
                            try {
                                cc = defaultMQAdminExt.examineConsumerConnectionInfo(consumerGroup);
                            } catch (Exception e) {
                                log.warn("examineConsumerConnectionInfo exception, " + consumerGroup, e);
                            }
                            GroupConsumeInfo groupConsumeInfo = new GroupConsumeInfo();
                            groupConsumeInfo.setGroup(consumerGroup);
                            if (consumeStats != null) {
                                groupConsumeInfo.setConsumeTps((int) consumeStats.getConsumeTps());
                                groupConsumeInfo.setDiffTotal(consumeStats.computeTotalDiff());
                            }
                            if (cc != null) {
                                groupConsumeInfo.setCount(cc.getConnectionSet().size());
                                groupConsumeInfo.setMessageModel(cc.getMessageModel());
                                groupConsumeInfo.setConsumeType(cc.getConsumeType());
                                groupConsumeInfo.setVersion(cc.computeMinVersion());
                            }
                            System.out.printf("%-64s  %-6d  %-24s %-5s  %-14s  %-7d  %d%n",
                                    UtilAll.frontStringAtLeast(groupConsumeInfo.getGroup(), 64),
                                    groupConsumeInfo.getCount(),
                                    groupConsumeInfo.getCount() > 0 ? groupConsumeInfo.versionDesc() : "OFFLINE",
                                    groupConsumeInfo.consumeTypeDesc(),
                                    groupConsumeInfo.messageModelDesc(),
                                    groupConsumeInfo.getConsumeTps(),
                                    groupConsumeInfo.getDiffTotal()
                            );
                        } catch (Exception e) {
                            log.warn("examineConsumeStats or examineConsumerConnectionInfo exception, " + consumerGroup, e);
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}

@Getter
@Setter
class GroupConsumeInfo implements Comparable<GroupConsumeInfo> {

    private String group;

    private int version;

    private int count;

    private ConsumeType consumeType;

    private MessageModel messageModel;

    private int consumeTps;

    private long diffTotal;

    public String consumeTypeDesc() {
        if (this.count != 0) {
            return this.getConsumeType() == ConsumeType.CONSUME_ACTIVELY ? "PULL" : "PUSH";
        }
        return "";
    }

    public String messageModelDesc() {
        if (this.count != 0 && this.getConsumeType() == ConsumeType.CONSUME_PASSIVELY) {
            return this.getMessageModel().toString();
        }
        return "";
    }

    public String versionDesc() {
        if (this.count != 0) {
            return MQVersion.getVersionDesc(this.version);
        }
        return "";
    }

    @Override
    public int compareTo(GroupConsumeInfo o) {
        if (this.count != o.count) {
            return o.count - this.count;
        }
        return (int) (o.diffTotal - diffTotal);
    }

}