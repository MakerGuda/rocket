package org.apache.rocketmq.client.consumer;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.filter.ExpressionType;

/**
 * 消息选择器，支持tag模式和sql92模式
 */
@Getter
@Setter
public class MessageSelector {

    /**
     * 过滤表达式类型
     */
    private String type;

    /**
     * 功率表达式
     */
    private String expression;

    private MessageSelector(String type, String expression) {
        this.type = type;
        this.expression = expression;
    }

    /**
     * 使用sql92选择消息
     */
    public static MessageSelector bySql(String sql) {
        return new MessageSelector(ExpressionType.SQL92, sql);
    }

    /**
     * 使用tag选择消息
     */
    public static MessageSelector byTag(String tag) {
        return new MessageSelector(ExpressionType.TAG, tag);
    }

}