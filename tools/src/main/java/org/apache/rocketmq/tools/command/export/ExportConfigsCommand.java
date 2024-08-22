package org.apache.rocketmq.tools.command.export;

import com.alibaba.fastjson.JSON;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.*;
import java.util.Map.Entry;

public class ExportConfigsCommand implements SubCommand {

    @Override
    public String commandName() {
        return "exportConfigs";
    }

    @Override
    public String commandDesc() {
        return "Export configs.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("c", "clusterName", true, "choose a cluster to export");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option("f", "filePath", true, "export configs.json path | default /tmp/rocketmq/export");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            String clusterName = commandLine.getOptionValue('c').trim();
            String filePath = !commandLine.hasOption('f') ? "/tmp/rocketmq/export" : commandLine.getOptionValue('f').trim();
            defaultMQAdminExt.start();
            Map<String, Object> result = new HashMap<>();
            List<String> nameServerAddressList = defaultMQAdminExt.getNameServerAddressList();
            int masterBrokerSize = 0;
            int slaveBrokerSize = 0;
            Map<String, Properties> brokerConfigs = new HashMap<>();
            Map<String, List<String>> masterAndSlaveMap = CommandUtil.fetchMasterAndSlaveDistinguish(defaultMQAdminExt, clusterName);
            for (Entry<String, List<String>> masterAndSlaveEntry : masterAndSlaveMap.entrySet()) {
                Properties masterProperties = defaultMQAdminExt.getBrokerConfig(masterAndSlaveEntry.getKey());
                masterBrokerSize++;
                slaveBrokerSize += masterAndSlaveEntry.getValue().size();
                brokerConfigs.put(masterProperties.getProperty("brokerName"), needBrokerProprties(masterProperties));
            }
            Map<String, Integer> clusterScaleMap = new HashMap<>();
            clusterScaleMap.put("namesrvSize", nameServerAddressList.size());
            clusterScaleMap.put("masterBrokerSize", masterBrokerSize);
            clusterScaleMap.put("slaveBrokerSize", slaveBrokerSize);
            result.put("brokerConfigs", brokerConfigs);
            result.put("clusterScale", clusterScaleMap);
            String path = filePath + "/configs.json";
            MixAll.string2FileNotSafe(JSON.toJSONString(result, true), path);
            System.out.printf("export %s success", path);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    private Properties needBrokerProprties(Properties properties) {
        List<String> propertyKeys = Arrays.asList(
                "brokerClusterName",
                "brokerId",
                "brokerName",
                "brokerRole",
                "fileReservedTime",
                "filterServerNums",
                "flushDiskType",
                "maxMessageSize",
                "messageDelayLevel",
                "msgTraceTopicName",
                "slaveReadEnable",
                "traceOn",
                "traceTopicEnable",
                "useTLS",
                "autoCreateTopicEnable",
                "autoCreateSubscriptionGroup"
        );

        Properties newProperties = new Properties();
        propertyKeys.stream().filter(key -> properties.getProperty(key) != null).forEach(key -> newProperties.setProperty(key, properties.getProperty(key)));
        return newProperties;
    }

}