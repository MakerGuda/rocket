package org.apache.rocketmq.broker.metrics;

import com.google.common.base.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;

@Getter
@Setter
public class ConsumerAttr {

    String group;

    LanguageCode language;

    int version;

    ConsumeType consumeMode;

    public ConsumerAttr(String group, LanguageCode language, int version, ConsumeType consumeMode) {
        this.group = group;
        this.language = language;
        this.version = version;
        this.consumeMode = consumeMode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ConsumerAttr attr = (ConsumerAttr) o;
        return version == attr.version && Objects.equal(group, attr.group) && language == attr.language && consumeMode == attr.consumeMode;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(group, language, version, consumeMode);
    }

}