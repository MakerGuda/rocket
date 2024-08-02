package org.apache.rocketmq.store.ha;

public enum HAConnectionState {

    /**
     * Ready to start connection.
     */
    READY,
    /**
     * CommitLog consistency checking.
     */
    HANDSHAKE,
    /**
     * Synchronizing data.
     */
    TRANSFER,
    /**
     * Temporarily stop transferring.
     */
    SUSPEND,
    /**
     * Connection shutdown.
     */
    SHUTDOWN,

}