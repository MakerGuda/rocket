package org.apache.rocketmq.example.operation;

import org.apache.commons.cli.*;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Consumer {

    public static void main(String[] args) throws MQClientException {
        CommandLine commandLine = buildCommandline(args);
        if (commandLine != null) {
            String subGroup = commandLine.getOptionValue('g');
            String topic = commandLine.getOptionValue('t');
            String subExpression = commandLine.getOptionValue('s');
            final String returnFailedHalf = commandLine.getOptionValue('f');
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(subGroup);
            consumer.setInstanceName(Long.toString(System.currentTimeMillis()));
            consumer.subscribe(topic, subExpression);
            consumer.registerMessageListener(new MessageListenerConcurrently() {
                final AtomicLong consumeTimes = new AtomicLong(0);
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                    long currentTimes = this.consumeTimes.incrementAndGet();
                    System.out.printf("%-8d %s%n", currentTimes, msgs);
                    if (Boolean.parseBoolean(returnFailedHalf)) {
                        if ((currentTimes % 2) == 0) {
                            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                        }
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            consumer.start();
            System.out.printf("Consumer Started.%n");
        }
    }

    public static CommandLine buildCommandline(String[] args) {
        final Options options = new Options();
        Option opt = new Option("h", "help", false, "Print help");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("g", "consumerGroup", true, "Consumer Group Name");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option("t", "topic", true, "Topic Name");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option("s", "subscription", true, "subscription");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("f", "returnFailedHalf", true, "return failed result, for half message");
        opt.setRequired(true);
        options.addOption(opt);
        DefaultParser parser = new DefaultParser();
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        CommandLine commandLine;
        try {
            commandLine = parser.parse(options, args);
            if (commandLine.hasOption('h')) {
                hf.printHelp("producer", options, true);
                return null;
            }
        } catch (ParseException e) {
            hf.printHelp("producer", options, true);
            return null;
        }
        return commandLine;
    }

}