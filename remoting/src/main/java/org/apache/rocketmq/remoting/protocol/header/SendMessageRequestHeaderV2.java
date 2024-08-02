package org.apache.rocketmq.remoting.protocol.header;

import com.google.common.base.MoreObjects;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.common.resource.RocketMQResource;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.FastCodesHeader;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.rpc.TopicQueueRequestHeader;

import java.util.HashMap;

@Getter
@Setter
@RocketMQAction(value = RequestCode.SEND_MESSAGE_V2, action = Action.PUB)
public class SendMessageRequestHeaderV2 extends TopicQueueRequestHeader implements CommandCustomHeader, FastCodesHeader {

    /**
     * 生产者组
     */
    @CFNotNull
    private String a;

    /**
     * 主题
     */
    @CFNotNull
    @RocketMQResource(ResourceType.TOPIC)
    private String b;

    /**
     * 默认主题
     */
    @CFNotNull
    private String c;

    /**
     * defaultTopicQueueNums
     */
    @CFNotNull
    private Integer d;

    /**
     * queueId
     */
    @CFNotNull
    private Integer e;

    /**
     * sysFlag
     */
    @CFNotNull
    private Integer f;

    /**
     * bornTimestamp
     */
    @CFNotNull
    private Long g;

    /**
     * flag
     */
    @CFNotNull
    private Integer h;

    /**
     * properties
     */
    @CFNullable
    private String i;

    /**
     * reconsumeTimes
     */
    @CFNullable
    private Integer j;

    /**
     * unitMode
     */
    @CFNullable
    private Boolean k;

    /**
     * consumeRetryTimes
     */
    private Integer l;

    /**
     * batch
     */
    @CFNullable
    private Boolean m;

    /**
     * brokerName
     */
    @CFNullable
    private String n;

    public static SendMessageRequestHeader createSendMessageRequestHeaderV1(final SendMessageRequestHeaderV2 v2) {
        SendMessageRequestHeader v1 = new SendMessageRequestHeader();
        v1.setProducerGroup(v2.a);
        v1.setTopic(v2.b);
        v1.setDefaultTopic(v2.c);
        v1.setDefaultTopicQueueNums(v2.d);
        v1.setQueueId(v2.e);
        v1.setSysFlag(v2.f);
        v1.setBornTimestamp(v2.g);
        v1.setFlag(v2.h);
        v1.setProperties(v2.i);
        v1.setReconsumeTimes(v2.j);
        v1.setUnitMode(v2.k);
        v1.setMaxReconsumeTimes(v2.l);
        v1.setBatch(v2.m);
        v1.setBrokerName(v2.n);
        return v1;
    }

    public static SendMessageRequestHeaderV2 createSendMessageRequestHeaderV2(final SendMessageRequestHeader v1) {
        SendMessageRequestHeaderV2 v2 = new SendMessageRequestHeaderV2();
        v2.a = v1.getProducerGroup();
        v2.b = v1.getTopic();
        v2.c = v1.getDefaultTopic();
        v2.d = v1.getDefaultTopicQueueNums();
        v2.e = v1.getQueueId();
        v2.f = v1.getSysFlag();
        v2.g = v1.getBornTimestamp();
        v2.h = v1.getFlag();
        v2.i = v1.getProperties();
        v2.j = v1.getReconsumeTimes();
        v2.k = v1.isUnitMode();
        v2.l = v1.getMaxReconsumeTimes();
        v2.m = v1.isBatch();
        v2.n = v1.getBrokerName();
        return v2;
    }

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    @Override
    public void encode(ByteBuf out) {
        writeIfNotNull(out, "a", a);
        writeIfNotNull(out, "b", b);
        writeIfNotNull(out, "c", c);
        writeIfNotNull(out, "d", d);
        writeIfNotNull(out, "e", e);
        writeIfNotNull(out, "f", f);
        writeIfNotNull(out, "g", g);
        writeIfNotNull(out, "h", h);
        writeIfNotNull(out, "i", i);
        writeIfNotNull(out, "j", j);
        writeIfNotNull(out, "k", k);
        writeIfNotNull(out, "l", l);
        writeIfNotNull(out, "m", m);
        writeIfNotNull(out, "n", n);
    }

    @Override
    public void decode(HashMap<String, String> fields) throws RemotingCommandException {
        String str = getAndCheckNotNull(fields, "a");
        if (str != null) {
            a = str;
        }
        str = getAndCheckNotNull(fields, "b");
        if (str != null) {
            b = str;
        }
        str = getAndCheckNotNull(fields, "c");
        if (str != null) {
            c = str;
        }
        str = getAndCheckNotNull(fields, "d");
        if (str != null) {
            d = Integer.parseInt(str);
        }
        str = getAndCheckNotNull(fields, "e");
        if (str != null) {
            e = Integer.parseInt(str);
        }
        str = getAndCheckNotNull(fields, "f");
        if (str != null) {
            f = Integer.parseInt(str);
        }
        str = getAndCheckNotNull(fields, "g");
        if (str != null) {
            g = Long.parseLong(str);
        }
        str = getAndCheckNotNull(fields, "h");
        if (str != null) {
            h = Integer.parseInt(str);
        }
        str = fields.get("i");
        if (str != null) {
            i = str;
        }
        str = fields.get("j");
        if (str != null) {
            j = Integer.parseInt(str);
        }
        str = fields.get("k");
        if (str != null) {
            k = Boolean.parseBoolean(str);
        }
        str = fields.get("l");
        if (str != null) {
            l = Integer.parseInt(str);
        }
        str = fields.get("m");
        if (str != null) {
            m = Boolean.parseBoolean(str);
        }
        str = fields.get("n");
        if (str != null) {
            n = str;
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("a", a)
                .add("b", b)
                .add("c", c)
                .add("d", d)
                .add("e", e)
                .add("f", f)
                .add("g", g)
                .add("h", h)
                .add("i", i)
                .add("j", j)
                .add("k", k)
                .add("l", l)
                .add("m", m)
                .add("n", n)
                .toString();
    }

    @Override
    public Integer getQueueId() {
        return e;
    }

    @Override
    public void setQueueId(Integer queueId) {
        this.e = queueId;
    }

    @Override
    public String getTopic() {
        return b;
    }

    @Override
    public void setTopic(String topic) {
        this.b = topic;
    }

}