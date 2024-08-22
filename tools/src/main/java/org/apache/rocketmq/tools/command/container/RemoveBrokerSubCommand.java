package org.apache.rocketmq.tools.command.container;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class RemoveBrokerSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "removeBroker";
    }

    @Override
    public String commandDesc() {
        return "Remove a broker from specified container.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("c", "brokerContainerAddr", true, "Broker container address");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option("b", "brokerIdentity", true, "Information to identify a broker: clusterName:brokerName:brokerId(dLedgerId for dLedger)");
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
            String[] brokerIdentities = commandLine.getOptionValue('b').trim().split(":");
            String clusterName = brokerIdentities[0].trim();
            String brokerName = brokerIdentities[1].trim();
            long brokerId;
            try {
                brokerId = Long.parseLong(brokerIdentities[2].trim());
            } catch (NumberFormatException e) {
                return;
            }
            if (brokerId < 0) {
                System.out.printf("brokerId can't be negative%n");
                return;
            }
            defaultMQAdminExt.removeBrokerFromContainer(brokerContainerAddr, clusterName, brokerName, brokerId);
            System.out.printf("remove broker from %s success%n", brokerContainerAddr);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

}