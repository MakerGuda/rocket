package org.apache.rocketmq.tools.command.namesrv;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

public class GetNamesrvConfigCommand implements SubCommand {

    @Override
    public String commandName() {
        return "getNamesrvConfig";
    }

    @Override
    public String commandDesc() {
        return "Get configs of name server.";
    }

    @Override
    public Options buildCommandlineOptions(final Options options) {
        return options;
    }

    @Override
    public void execute(final CommandLine commandLine, final Options options, final RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            String servers = commandLine.getOptionValue('n');
            List<String> serverList = null;
            if (servers != null && !servers.isEmpty()) {
                String[] serverArray = servers.trim().split(";");
                if (serverArray.length > 0) {
                    serverList = Arrays.asList(serverArray);
                }
            }
            defaultMQAdminExt.start();
            Map<String, Properties> nameServerConfigs = defaultMQAdminExt.getNameServerConfig(serverList);

            for (Entry<String, Properties> nameServerConfigEntry : nameServerConfigs.entrySet()) {
                System.out.printf("============%s============\n", nameServerConfigEntry.getKey());
                for (Entry<Object, Object> entry : nameServerConfigEntry.getValue().entrySet()) {
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