package org.apache.rocketmq.tools.command.queue;

import com.alibaba.fastjson.JSON;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.body.ConsumeQueueData;
import org.apache.rocketmq.remoting.protocol.body.QueryConsumeQueueResponseBody;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;

public class QueryConsumeQueueCommand implements SubCommand {

    @Override
    public String commandName() {
        return "queryCq";
    }

    @Override
    public String commandDesc() {
        return "Query cq command.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option("q", "queue", true, "queue num, ie. 1");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option("i", "index", true, "start queue index.");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option("c", "count", true, "how many.");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("b", "broker", true, "broker addr.");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("g", "consumer", true, "consumer group.");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            defaultMQAdminExt.start();
            String topic = commandLine.getOptionValue("t").trim();
            int queueId = Integer.parseInt(commandLine.getOptionValue("q").trim());
            long index = Long.parseLong(commandLine.getOptionValue("i").trim());
            int count = Integer.parseInt(commandLine.getOptionValue("c", "10").trim());
            String broker = null;
            if (commandLine.hasOption("b")) {
                broker = commandLine.getOptionValue("b").trim();
            }
            String consumerGroup = null;
            if (commandLine.hasOption("g")) {
                consumerGroup = commandLine.getOptionValue("g").trim();
            }
            if (StringUtils.isEmpty(broker)) {
                TopicRouteData topicRouteData = defaultMQAdminExt.examineTopicRouteInfo(topic);
                if (topicRouteData == null || topicRouteData.getBrokerDatas() == null || topicRouteData.getBrokerDatas().isEmpty()) {
                    throw new Exception("No topic route data!");
                }
                broker = topicRouteData.getBrokerDatas().get(0).getBrokerAddrs().get(0L);
            }
            QueryConsumeQueueResponseBody queryConsumeQueueResponseBody = defaultMQAdminExt.queryConsumeQueue(broker, topic, queueId, index, count, consumerGroup);
            if (queryConsumeQueueResponseBody.getSubscriptionData() != null) {
                System.out.printf("Subscription data: \n%s\n", JSON.toJSONString(queryConsumeQueueResponseBody.getSubscriptionData(), true));
                System.out.print("======================================\n");
            }
            if (queryConsumeQueueResponseBody.getFilterData() != null) {
                System.out.printf("Filter data: \n%s\n", queryConsumeQueueResponseBody.getFilterData());
                System.out.print("======================================\n");
            }
            System.out.printf("Queue data: \nmax: %d, min: %d\n", queryConsumeQueueResponseBody.getMaxQueueIndex(), queryConsumeQueueResponseBody.getMinQueueIndex());
            System.out.print("======================================\n");
            if (queryConsumeQueueResponseBody.getQueueData() != null) {
                long i = index;
                for (ConsumeQueueData queueData : queryConsumeQueueResponseBody.getQueueData()) {
                    String stringBuilder = "idx: " + i + "\n" + queueData.toString() + "\n" + "======================================\n";
                    System.out.print(stringBuilder);
                    i++;
                }
            }
        } catch (Exception ignore) {
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

}