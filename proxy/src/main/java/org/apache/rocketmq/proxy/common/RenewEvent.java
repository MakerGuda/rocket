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

package org.apache.rocketmq.proxy.common;

import org.apache.rocketmq.client.consumer.AckResult;

import java.util.concurrent.CompletableFuture;

public class RenewEvent {
    protected ReceiptHandleGroupKey key;
    protected MessageReceiptHandle messageReceiptHandle;
    protected long renewTime;
    protected EventType eventType;
    protected CompletableFuture<AckResult> future;

    public RenewEvent(ReceiptHandleGroupKey key, MessageReceiptHandle messageReceiptHandle, long renewTime,
                      EventType eventType, CompletableFuture<AckResult> future) {
        this.key = key;
        this.messageReceiptHandle = messageReceiptHandle;
        this.renewTime = renewTime;
        this.eventType = eventType;
        this.future = future;
    }

    public ReceiptHandleGroupKey getKey() {
        return key;
    }

    public MessageReceiptHandle getMessageReceiptHandle() {
        return messageReceiptHandle;
    }

    public long getRenewTime() {
        return renewTime;
    }

    public EventType getEventType() {
        return eventType;
    }

    public CompletableFuture<AckResult> getFuture() {
        return future;
    }

    public enum EventType {
        RENEW,
        STOP_RENEW,
        CLEAR_GROUP
    }
}
