package org.apache.rocketmq.tools.command.controller;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class ReElectMasterSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "electMaster";
    }

    @Override
    public String commandDesc() {
        return "Re-elect the specified broker as master.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("a", "controllerAddress", true, "The address of controller");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option("b", "brokerId", true, "The id of the broker which requires to become master");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option("bn", "brokerName", true, "The broker name of the replicas that require to be manipulated");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option("c", "clusterName", true, "the clusterName of broker");
        opt.setRequired(true);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        String controllerAddress = commandLine.getOptionValue("a").trim();
        String clusterName = commandLine.getOptionValue('c').trim();
        String brokerName = commandLine.getOptionValue("bn").trim();
        Long brokerId = Long.valueOf(commandLine.getOptionValue("b").trim());
        try {
            defaultMQAdminExt.start();
            final Pair<ElectMasterResponseHeader, BrokerMemberGroup> pair = defaultMQAdminExt.electMaster(controllerAddress, clusterName, brokerName, brokerId);
            final ElectMasterResponseHeader metaData = pair.getObject1();
            final BrokerMemberGroup brokerMemberGroup = pair.getObject2();
            System.out.printf("\n#ClusterName\t%s", clusterName);
            System.out.printf("\n#BrokerName\t%s", brokerName);
            System.out.printf("\n#BrokerMasterAddr\t%s", metaData.getMasterAddress());
            System.out.printf("\n#MasterEpoch\t%s", metaData.getMasterEpoch());
            System.out.printf("\n#SyncStateSetEpoch\t%s\n", metaData.getSyncStateSetEpoch());
            if (null != brokerMemberGroup && null != brokerMemberGroup.getBrokerAddrs()) {
                brokerMemberGroup.getBrokerAddrs().forEach((key, value) -> System.out.printf("\t#Broker\t%d\t%s\n", key, value));
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

}