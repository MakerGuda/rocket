package org.apache.rocketmq.common.message;

import lombok.Getter;

import java.nio.ByteBuffer;

@Getter
public enum MessageVersion {

    MESSAGE_VERSION_V1(MessageDecoder.MESSAGE_MAGIC_CODE) {
        @Override
        public int getTopicLengthSize() {
            return 1;
        }

        @Override
        public int getTopicLength(ByteBuffer buffer) {
            return buffer.get();
        }

        @Override
        public void putTopicLength(ByteBuffer buffer, int topicLength) {
            buffer.put((byte) topicLength);
        }
    },

    MESSAGE_VERSION_V2(MessageDecoder.MESSAGE_MAGIC_CODE_V2) {
        @Override
        public int getTopicLengthSize() {
            return 2;
        }

        @Override
        public int getTopicLength(ByteBuffer buffer) {
            return buffer.getShort();
        }

        @Override
        public void putTopicLength(ByteBuffer buffer, int topicLength) {
            buffer.putShort((short) topicLength);
        }
    };

    private final int magicCode;

    MessageVersion(int magicCode) {
        this.magicCode = magicCode;
    }

    public static MessageVersion valueOfMagicCode(int magicCode) {
        for (MessageVersion version : MessageVersion.values()) {
            if (version.getMagicCode() == magicCode) {
                return version;
            }
        }
        throw new IllegalArgumentException("Invalid magicCode " + magicCode);
    }

    public abstract int getTopicLengthSize();

    public abstract int getTopicLength(java.nio.ByteBuffer buffer);

    public abstract void putTopicLength(java.nio.ByteBuffer buffer, int topicLength);

}