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
import java.util.Map;
import java.util.Properties;

public class GetControllerConfigSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "getControllerConfig";
    }

    @Override
    public String commandDesc() {
        return "Get controller config.";
    }

    @Override
    public Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("a", "controllerAddress", true, "Controller address list, eg: 192.168.0.1:9878;192.168.0.2:9878");
        opt.setRequired(true);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(final CommandLine commandLine, final Options options, final RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            String servers = commandLine.getOptionValue('a');
            List<String> serverList = null;
            if (servers != null && !servers.isEmpty()) {
                String[] serverArray = servers.trim().split(";");
                if (serverArray.length > 0) {
                    serverList = Arrays.asList(serverArray);
                }
            }
            defaultMQAdminExt.start();
            Map<String, Properties> controllerConfigs = defaultMQAdminExt.getControllerConfig(serverList);
            for (Map.Entry<String, Properties> controllerConfigEntry : controllerConfigs.entrySet()) {
                System.out.printf("============%s============\n", controllerConfigEntry.getKey());
                for (Map.Entry<Object, Object> entry : controllerConfigEntry.getValue().entrySet()) {
                    System.out.printf("%-50s=  %s\n", entry.getKey(), entry.getValue());
                }
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

}