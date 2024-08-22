package org.apache.rocketmq.tools.command.consumer;

import com.alibaba.fastjson.JSON;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.attribute.AttributeParser;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.subscription.GroupRetryPolicy;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.Map;
import java.util.Set;

public class UpdateSubGroupSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "updateSubGroup";
    }

    @Override
    public String commandDesc() {
        return "Update or create subscription group.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerAddr", true, "create subscription group to which broker");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("c", "clusterName", true, "create subscription group to which cluster");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("g", "groupName", true, "consumer group name");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option("s", "consumeEnable", true, "consume enable");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("m", "consumeFromMinEnable", true, "from min offset");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("d", "consumeBroadcastEnable", true, "broadcast");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("o", "consumeMessageOrderly", true, "consume message orderly");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("q", "retryQueueNums", true, "retry queue nums");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("r", "retryMaxTimes", true, "retry max times");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("p", "groupRetryPolicy", true,
                "the json string of retry policy ( exp: " +
                        "{\"type\":\"EXPONENTIAL\",\"exponentialRetryPolicy\":{\"initial\":5000,\"max\":7200000,\"multiplier\":2}} " +
                        "{\"type\":\"CUSTOMIZED\",\"customizedRetryPolicy\":{\"next\":[1000,5000,10000]}} )");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("i", "brokerId", true, "consumer from which broker id");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("w", "whichBrokerWhenConsumeSlowly", true, "which broker id when consume slowly");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("a", "notifyConsumerIdsChanged", true, "notify consumerId changed");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option(null, "attributes", true, "attribute(+a=b,+c=d,-e)");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(final CommandLine commandLine, final Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setConsumeBroadcastEnable(false);
            subscriptionGroupConfig.setConsumeFromMinEnable(false);
            subscriptionGroupConfig.setGroupName(commandLine.getOptionValue('g').trim());
            if (commandLine.hasOption('s')) {
                subscriptionGroupConfig.setConsumeEnable(Boolean.parseBoolean(commandLine.getOptionValue('s').trim()));
            }
            if (commandLine.hasOption('m')) {
                subscriptionGroupConfig.setConsumeFromMinEnable(Boolean.parseBoolean(commandLine.getOptionValue('m').trim()));
            }
            if (commandLine.hasOption('d')) {
                subscriptionGroupConfig.setConsumeBroadcastEnable(Boolean.parseBoolean(commandLine.getOptionValue('d').trim()));
            }
            if (commandLine.hasOption('o')) {
                subscriptionGroupConfig.setConsumeMessageOrderly(Boolean.parseBoolean(commandLine.getOptionValue('o').trim()));
            }
            if (commandLine.hasOption('q')) {
                subscriptionGroupConfig.setRetryQueueNums(Integer.parseInt(commandLine.getOptionValue('q').trim()));
            }
            if (commandLine.hasOption('r')) {
                subscriptionGroupConfig.setRetryMaxTimes(Integer.parseInt(commandLine.getOptionValue('r').trim()));
            }
            if (commandLine.hasOption('p')) {
                String groupRetryPolicyJson = commandLine.getOptionValue('p').trim();
                GroupRetryPolicy groupRetryPolicy = JSON.parseObject(groupRetryPolicyJson, GroupRetryPolicy.class);
                subscriptionGroupConfig.setGroupRetryPolicy(groupRetryPolicy);
            }
            if (commandLine.hasOption('i')) {
                subscriptionGroupConfig.setBrokerId(Long.parseLong(commandLine.getOptionValue('i').trim()));
            }
            if (commandLine.hasOption('w')) {
                subscriptionGroupConfig.setWhichBrokerWhenConsumeSlowly(Long.parseLong(commandLine.getOptionValue('w').trim()));
            }
            if (commandLine.hasOption('a')) {
                subscriptionGroupConfig.setNotifyConsumerIdsChangedEnable(Boolean.parseBoolean(commandLine.getOptionValue('a').trim()));
            }
            if (commandLine.hasOption("attributes")) {
                String attributesModification = commandLine.getOptionValue("attributes").trim();
                Map<String, String> attributes = AttributeParser.parseToMap(attributesModification);
                subscriptionGroupConfig.setAttributes(attributes);
            }
            if (commandLine.hasOption('b')) {
                String addr = commandLine.getOptionValue('b').trim();
                defaultMQAdminExt.start();
                defaultMQAdminExt.createAndUpdateSubscriptionGroupConfig(addr, subscriptionGroupConfig);
                System.out.printf("create subscription group to %s success.%n", addr);
                System.out.printf("%s", subscriptionGroupConfig);
                return;
            } else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();
                defaultMQAdminExt.start();
                Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String addr : masterSet) {
                    try {
                        defaultMQAdminExt.createAndUpdateSubscriptionGroupConfig(addr, subscriptionGroupConfig);
                        System.out.printf("create subscription group to %s success.%n", addr);
                    } catch (Exception e) {
                        Thread.sleep(1000);
                    }
                }
                System.out.printf("%s", subscriptionGroupConfig);
                return;
            }
            ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

}