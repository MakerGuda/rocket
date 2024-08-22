package org.apache.rocketmq.tools.command.consumer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.body.Connection;
import org.apache.rocketmq.remoting.protocol.body.ConsumerConnection;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.Map.Entry;
import java.util.TreeMap;

public class ConsumerStatusSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "consumerStatus";
    }

    @Override
    public String commandDesc() {
        return "Query consumer's internal data structure.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("g", "consumerGroup", true, "consumer group name");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option("i", "clientId", true, "The consumer's client id");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("b", "brokerAddr", true, "broker address");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("s", "jstack", false, "Run jstack command in the consumer progress");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
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
            String group = commandLine.getOptionValue('g').trim();
            ConsumerConnection cc = commandLine.hasOption('b') ? defaultMQAdminExt.examineConsumerConnectionInfo(group, commandLine.getOptionValue('b').trim()) : defaultMQAdminExt.examineConsumerConnectionInfo(group);
            boolean jstack = commandLine.hasOption('s');
            if (!commandLine.hasOption('i')) {
                int i = 1;
                long now = System.currentTimeMillis();
                final TreeMap<String, ConsumerRunningInfo> criTable = new TreeMap<>();
                System.out.printf("%-10s %-40s %-20s %s%n", "#Index", "#ClientId", "#Version", "#ConsumerRunningInfoFile");
                for (Connection conn : cc.getConnectionSet()) {
                    try {
                        ConsumerRunningInfo consumerRunningInfo = defaultMQAdminExt.getConsumerRunningInfo(group, conn.getClientId(), jstack);
                        if (consumerRunningInfo != null) {
                            criTable.put(conn.getClientId(), consumerRunningInfo);
                            String filePath = now + "/" + conn.getClientId();
                            MixAll.string2FileNotSafe(consumerRunningInfo.formatString(), filePath);
                            System.out.printf("%-10d %-40s %-20s %s%n", i++, conn.getClientId(), MQVersion.getVersionDesc(conn.getVersion()), filePath);
                        }
                    } catch (Exception ignore) {
                    }
                }
                if (!criTable.isEmpty()) {
                    boolean subSame = ConsumerRunningInfo.analyzeSubscription(criTable);
                    boolean rebalanceOK = subSame && ConsumerRunningInfo.analyzeRebalance(criTable);
                    if (subSame) {
                        System.out.printf("%n%nSame subscription in the same group of consumer");
                        System.out.printf("%n%nRebalance %s%n", rebalanceOK ? "OK" : "Failed");
                        for (Entry<String, ConsumerRunningInfo> next : criTable.entrySet()) {
                            String result = ConsumerRunningInfo.analyzeProcessQueue(next.getKey(), next.getValue());
                            if (!result.isEmpty()) {
                                System.out.printf("%s", result);
                            }
                        }
                    } else {
                        System.out.printf("%n%nWARN: Different subscription in the same group of consumer!!!");
                    }
                }
            } else {
                String clientId = commandLine.getOptionValue('i').trim();
                ConsumerRunningInfo consumerRunningInfo = defaultMQAdminExt.getConsumerRunningInfo(group, clientId, jstack);
                if (consumerRunningInfo != null) {
                    System.out.printf("%s", consumerRunningInfo.formatString());
                }
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

}