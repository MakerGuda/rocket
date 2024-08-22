package org.apache.rocketmq.tools.command.topic;

import lombok.Getter;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
public class RebalanceResult {

    private final Map<String, List<MessageQueue>> result = new HashMap<>();

}