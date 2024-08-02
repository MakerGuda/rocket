package org.apache.rocketmq.common.message;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.utils.MessageUtils;

import java.nio.ByteBuffer;

@Getter
@Setter
public class MessageExtBrokerInner extends MessageExt {

    private static final long serialVersionUID = 7256001576878700634L;

    private String propertiesString;

    private long tagsCode;

    private ByteBuffer encodedBuff;

    private volatile boolean encodeCompleted;

    private MessageVersion version = MessageVersion.MESSAGE_VERSION_V1;

    public static long tagsString2tagsCode(final String tags) {
        if (null == tags || tags.isEmpty()) {
            return 0;
        }
        return tags.hashCode();
    }

    public void deleteProperty(String name) {
        super.clearProperty(name);
        if (propertiesString != null) {
            this.setPropertiesString(MessageUtils.deleteProperty(propertiesString, name));
        }
    }

    public void removeWaitStorePropertyString() {
        if (this.getProperties().containsKey(MessageConst.PROPERTY_WAIT_STORE_MSG_OK)) {
            String waitStoreMsgOKValue = this.getProperties().remove(MessageConst.PROPERTY_WAIT_STORE_MSG_OK);
            this.setPropertiesString(MessageDecoder.messageProperties2String(this.getProperties()));
            this.getProperties().put(MessageConst.PROPERTY_WAIT_STORE_MSG_OK, waitStoreMsgOKValue);
        } else {
            this.setPropertiesString(MessageDecoder.messageProperties2String(this.getProperties()));
        }
    }

}