package org.apache.rocketmq.tools.command.auth;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.Set;

public class DeleteAclSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "deleteAcl";
    }

    @Override
    public String commandDesc() {
        return "Delete acl from cluster.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        OptionGroup optionGroup = new OptionGroup();
        Option opt = new Option("c", "clusterName", true, "delete acl from which cluster");
        optionGroup.addOption(opt);
        opt = new Option("b", "brokerAddr", true, "delete acl from which broker");
        optionGroup.addOption(opt);
        optionGroup.setRequired(true);
        options.addOptionGroup(optionGroup);
        opt = new Option("s", "subject", true, "the subject of acl to delete.");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option("r", "resources", true, "the resources of acl to delete");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            String subject = null;
            if (commandLine.hasOption("s")) {
                subject = StringUtils.trim(commandLine.getOptionValue("s"));
            }
            String resource = null;
            if (commandLine.hasOption('r')) {
                resource = StringUtils.trim(commandLine.getOptionValue("r"));
            }
            if (commandLine.hasOption('b')) {
                String addr = StringUtils.trim(commandLine.getOptionValue('b'));
                defaultMQAdminExt.start();
                defaultMQAdminExt.deleteAcl(addr, subject, resource);
                System.out.printf("delete acl to %s success.%n", addr);
                return;
            } else if (commandLine.hasOption('c')) {
                String clusterName = StringUtils.trim(commandLine.getOptionValue('c'));
                defaultMQAdminExt.start();
                Set<String> brokerAddrSet = CommandUtil.fetchMasterAndSlaveAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String addr : brokerAddrSet) {
                    defaultMQAdminExt.deleteAcl(addr, subject, resource);
                    System.out.printf("delete acl to %s success.%n", addr);
                }
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