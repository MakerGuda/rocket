
package org.apache.rocketmq.tools.command.controller;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class UpdateControllerConfigSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "updateControllerConfig";
    }

    @Override
    public String commandDesc() {
        return "Update controller config.";
    }

    @Override
    public Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("a", "controllerAddress", true, "Controller address list, eg: 192.168.0.1:9878;192.168.0.2:9878");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option("k", "key", true, "config key");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option("v", "value", true, "config value");
        opt.setRequired(true);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(final CommandLine commandLine, final Options options, final RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            String key = commandLine.getOptionValue('k').trim();
            String value = commandLine.getOptionValue('v').trim();
            Properties properties = new Properties();
            properties.put(key, value);
            String servers = commandLine.getOptionValue('a');
            List<String> serverList = null;
            if (servers != null && !servers.isEmpty()) {
                String[] serverArray = servers.trim().split(";");
                if (serverArray.length > 0) {
                    serverList = Arrays.asList(serverArray);
                }
            }
            defaultMQAdminExt.start();
            defaultMQAdminExt.updateControllerConfig(properties, serverList);
            System.out.printf("update controller config success!%s\n%s : %s\n", serverList == null ? "" : serverList, key, value);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

}