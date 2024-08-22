package org.apache.rocketmq.tools.command.broker;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class ResetMasterFlushOffsetSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "resetMasterFlushOffset";
    }

    @Override
    public String commandDesc() {
        return "Reset master flush offset in slave.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerAddr", true, "which broker to reset");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("o", "offset", true, "the offset to reset at");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            defaultMQAdminExt.start();
            String brokerAddr = commandLine.getOptionValue('b').trim();
            long masterFlushOffset = Long.parseLong(commandLine.getOptionValue('o').trim());
            defaultMQAdminExt.resetMasterFlushOffset(brokerAddr, masterFlushOffset);
            System.out.printf("reset master flush offset to %d success%n", masterFlushOffset);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

}