package org.apache.rocketmq.tools.command.broker;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class DeleteExpiredCommitLogSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "deleteExpiredCommitLog";
    }

    @Override
    public String commandDesc() {
        return "Delete expired CommitLog files.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("n", "namesrvAddr", true, "Name server address");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("b", "brokerAddr", true, "Broker address");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("c", "cluster", true, "clustername");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        if (commandLine.hasOption('n')) {
            defaultMQAdminExt.setNamesrvAddr(commandLine.getOptionValue('n').trim());
        }
        try {
            boolean result;
            defaultMQAdminExt.start();
            if (commandLine.hasOption('b')) {
                String addr = commandLine.getOptionValue('b').trim();
                result = defaultMQAdminExt.deleteExpiredCommitLogByAddr(addr);
            } else {
                String cluster = commandLine.getOptionValue('c');
                if (null != cluster)
                    cluster = cluster.trim();
                result = defaultMQAdminExt.deleteExpiredCommitLog(cluster);
            }
            System.out.printf(result ? "success" : "false");
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command execute failed.", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

}