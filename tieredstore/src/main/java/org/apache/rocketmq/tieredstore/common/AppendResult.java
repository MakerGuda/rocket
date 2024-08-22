package org.apache.rocketmq.tieredstore.common;

public enum AppendResult {

    /**
     * The append operation was successful.
     */
    SUCCESS,

    /**
     * The buffer used for the append operation is full.
     */
    BUFFER_FULL,

    /**
     * The file is full and cannot accept more data.
     */
    FILE_FULL,

    /**
     * The file is closed and cannot accept more data.
     */
    FILE_CLOSED,

    /**
     * An unknown error occurred during the append operation.
     */
    UNKNOWN_ERROR

}