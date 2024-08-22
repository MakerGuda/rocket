package org.apache.rocketmq.tieredstore.common;

import lombok.Getter;

import java.util.Arrays;

@Getter
public enum FileSegmentType {

    COMMIT_LOG(0),

    CONSUME_QUEUE(1),

    INDEX(2);

    private final int code;

    FileSegmentType(int code) {
        this.code = code;
    }

    public static FileSegmentType valueOf(int fileType) {
        return Arrays.stream(FileSegmentType.values()).filter(segmentType -> segmentType.getCode() == fileType).findFirst().orElse(COMMIT_LOG);
    }

}