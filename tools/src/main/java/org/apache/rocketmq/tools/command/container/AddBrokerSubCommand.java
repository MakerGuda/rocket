package org.apache.rocketmq.tools.command.container;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class AddBrokerSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "addBroker";
    }

    @Override
    public String commandDesc() {
        return "Add a broker to specified container.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("c", "brokerContainerAddr", true, "Broker container address");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option("b", "brokerConfigPath", true, "Broker config path");
        opt.setRequired(true);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            defaultMQAdminExt.start();
            String brokerContainerAddr = commandLine.getOptionValue('c').trim();
            String brokerConfigPath = commandLine.getOptionValue('b').trim();
            defaultMQAdminExt.addBrokerToContainer(brokerContainerAddr, brokerConfigPath);
            System.out.printf("add broker to %s success%n", brokerContainerAddr);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

}