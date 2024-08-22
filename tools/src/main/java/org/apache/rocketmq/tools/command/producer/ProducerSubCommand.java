package org.apache.rocketmq.tools.command.producer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.body.ProducerInfo;
import org.apache.rocketmq.remoting.protocol.body.ProducerTableInfo;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.List;

public class ProducerSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "producer";
    }

    @Override
    public String commandDesc() {
        return "Query producer's instances, connection, status, etc.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "broker", true, "broker address");
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
            String brokerAddr = commandLine.getOptionValue('b').trim();
            ProducerTableInfo cc = defaultMQAdminExt.getAllProducerInfo(brokerAddr);
            if (cc != null && cc.getData() != null && !cc.getData().isEmpty()) {
                for (String group : cc.getData().keySet()) {
                    List<ProducerInfo> list = cc.getData().get(group);
                    if (list == null || list.isEmpty()) {
                        System.out.printf("producer group (%s) instances are empty\n", group);
                        continue;
                    }
                    for (ProducerInfo producer : list) {
                        System.out.printf("producer group (%s) instance : %s\n", group, producer.toString());
                    }
                }
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

}