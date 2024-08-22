package org.apache.rocketmq.tools.command.acl;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class UpdateAccessConfigSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "updateAclConfig";
    }

    @Override
    public String commandDesc() {
        return "Update acl config yaml file in broker.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        OptionGroup optionGroup = new OptionGroup();
        Option opt = new Option("b", "brokerAddr", true, "update acl config file to which broker");
        optionGroup.addOption(opt);
        opt = new Option("c", "clusterName", true, "update acl config file to which cluster");
        optionGroup.addOption(opt);
        optionGroup.setRequired(true);
        options.addOptionGroup(optionGroup);
        opt = new Option("a", "accessKey", true, "set accessKey in acl config file");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option("s", "secretKey", true, "set secretKey in acl config file");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option("w", "whiteRemoteAddress", true, "set white ip Address for account in acl config file");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("i", "defaultTopicPerm", true, "set default topicPerm in acl config file");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("u", "defaultGroupPerm", true, "set default GroupPerm in acl config file");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("t", "topicPerms", true, "set topicPerms list,eg: topicA=DENY,topicD=SUB");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("g", "groupPerms", true, "set groupPerms list,eg: groupD=DENY,groupD=SUB");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("m", "admin", true, "set admin flag in acl config file");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            PlainAccessConfig accessConfig = new PlainAccessConfig();
            accessConfig.setAccessKey(commandLine.getOptionValue('a').trim());
            if (commandLine.hasOption('s')) {
                accessConfig.setSecretKey(commandLine.getOptionValue('s').trim());
            }
            if (commandLine.hasOption('m')) {
                accessConfig.setAdmin(Boolean.parseBoolean(commandLine.getOptionValue('m').trim()));
            }
            if (commandLine.hasOption('i')) {
                accessConfig.setDefaultTopicPerm(commandLine.getOptionValue('i').trim());
            }
            if (commandLine.hasOption('u')) {
                accessConfig.setDefaultGroupPerm(commandLine.getOptionValue('u').trim());
            }
            if (commandLine.hasOption('w')) {
                accessConfig.setWhiteRemoteAddress(commandLine.getOptionValue('w').trim());
            }
            if (commandLine.hasOption('t')) {
                String[] topicPerms = commandLine.getOptionValue('t').trim().split(",");
                List<String> topicPermList = new ArrayList<>();
                Collections.addAll(topicPermList, topicPerms);
                accessConfig.setTopicPerms(topicPermList);
            }
            if (commandLine.hasOption('g')) {
                String[] groupPerms = commandLine.getOptionValue('g').trim().split(",");
                List<String> groupPermList = new ArrayList<>();
                Collections.addAll(groupPermList, groupPerms);
                accessConfig.setGroupPerms(groupPermList);
            }
            if (commandLine.hasOption('b')) {
                String addr = commandLine.getOptionValue('b').trim();
                defaultMQAdminExt.start();
                defaultMQAdminExt.createAndUpdatePlainAccessConfig(addr, accessConfig);
                System.out.printf("create or update plain access config to %s success.%n", addr);
                System.out.printf("%s", accessConfig);
                return;
            } else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();
                defaultMQAdminExt.start();
                Set<String> brokerAddrSet = CommandUtil.fetchMasterAndSlaveAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String addr : brokerAddrSet) {
                    defaultMQAdminExt.createAndUpdatePlainAccessConfig(addr, accessConfig);
                    System.out.printf("create or update plain access config to %s success.%n", addr);
                }
                System.out.printf("%s", accessConfig);
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