package org.apache.rocketmq.store;

import lombok.Getter;

@Getter
public enum StoreType {

    DEFAULT("default"),
    DEFAULT_ROCKSDB("defaultRocksDB");

    private final String storeType;

    StoreType(String storeType) {
        this.storeType = storeType;
    }

}