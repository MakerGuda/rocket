package org.apache.rocketmq.tools.admin.api;

import lombok.Data;

import java.util.List;

@Data
public class BrokerOperatorResult {

    /**
     * 成功列表
     */
    private List<String> successList;

    /**
     * 失败列表
     */
    private List<String> failureList;

}