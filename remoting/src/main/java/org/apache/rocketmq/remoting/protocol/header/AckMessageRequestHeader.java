/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.remoting.protocol.header;

import com.google.common.base.MoreObjects;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.common.resource.RocketMQResource;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.rpc.TopicQueueRequestHeader;

@RocketMQAction(value = RequestCode.ACK_MESSAGE, action = Action.SUB)
public class AckMessageRequestHeader extends TopicQueueRequestHeader {
    @CFNotNull
    @RocketMQResource(ResourceType.GROUP)
    private String consumerGroup;
    @CFNotNull
    @RocketMQResource(ResourceType.TOPIC)
    private String topic;
    @CFNotNull
    private Integer queueId;
    @CFNotNull
    private String extraInfo;

    @CFNotNull
    private Long offset;


    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getExtraInfo() {
        return extraInfo;
    }

    public void setExtraInfo(String extraInfo) {
        this.extraInfo = extraInfo;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("consumerGroup", consumerGroup)
                .add("topic", topic)
                .add("queueId", queueId)
                .add("extraInfo", extraInfo)
                .add("offset", offset)
                .toString();
    }
}
