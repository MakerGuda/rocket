package org.apache.rocketmq.remoting;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.remoting.protocol.DataVersion;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Configuration {

    private final Logger log;

    private final List<Object> configObjectList = new ArrayList<>(4);

    private String storePath;

    private boolean storePathFromConfig = false;

    private Object storePathObject;

    private Field storePathField;

    private final DataVersion dataVersion = new DataVersion();

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private final Properties allConfigs = new Properties();

    public Configuration(Logger log) {
        this.log = log;
    }

    public Configuration(Logger log, Object... configObjects) {
        this.log = log;
        if (configObjects == null) {
            return;
        }
        for (Object configObject : configObjects) {
            if (configObject == null) {
                continue;
            }
            registerConfig(configObject);
        }
    }

    public Configuration(Logger log, String storePath, Object... configObjects) {
        this(log, configObjects);
        this.storePath = storePath;
    }

    public void registerConfig(Object configObject) {
        try {
            readWriteLock.writeLock().lockInterruptibly();
            try {
                Properties registerProps = MixAll.object2Properties(configObject);
                merge(registerProps, this.allConfigs);
                configObjectList.add(configObject);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("registerConfig lock error");
        }
    }

    public void registerConfig(Properties extProperties) {
        if (extProperties == null) {
            return;
        }
        try {
            readWriteLock.writeLock().lockInterruptibly();
            try {
                merge(extProperties, this.allConfigs);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("register lock error. {}" + extProperties);
        }
    }

    public void setStorePathFromConfig(Object object, String fieldName) {
        assert object != null;
        try {
            readWriteLock.writeLock().lockInterruptibly();
            try {
                this.storePathFromConfig = true;
                this.storePathObject = object;
                this.storePathField = object.getClass().getDeclaredField(fieldName);
                assert !Modifier.isStatic(this.storePathField.getModifiers());
                this.storePathField.setAccessible(true);
            } catch (NoSuchFieldException e) {
                throw new RuntimeException(e);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("setStorePathFromConfig lock error");
        }
    }

    private String getStorePath() {
        String realStorePath = null;
        try {
            readWriteLock.readLock().lockInterruptibly();
            try {
                realStorePath = this.storePath;
                if (this.storePathFromConfig) {
                    try {
                        realStorePath = (String) storePathField.get(this.storePathObject);
                    } catch (IllegalAccessException e) {
                        log.error("getStorePath error, ", e);
                    }
                }
            } finally {
                readWriteLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getStorePath lock error");
        }
        return realStorePath;
    }

    public void setStorePath(final String storePath) {
        this.storePath = storePath;
    }

    public void update(Properties properties) {
        try {
            readWriteLock.writeLock().lockInterruptibly();
            try {
                mergeIfExist(properties, this.allConfigs);
                for (Object configObject : configObjectList) {
                    MixAll.properties2Object(properties, configObject);
                }
                this.dataVersion.nextVersion();
            } finally {
                readWriteLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("update lock error, {}", properties);
            return;
        }
        persist();
    }

    public void persist() {
        try {
            readWriteLock.readLock().lockInterruptibly();
            try {
                String allConfigs = getAllConfigsInternal();
                MixAll.string2File(allConfigs, getStorePath());
            } catch (IOException e) {
                log.error("persist string2File error, ", e);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("persist lock error");
        }
    }

    public String getAllConfigsFormatString() {
        try {
            readWriteLock.readLock().lockInterruptibly();
            try {
                return getAllConfigsInternal();
            } finally {
                readWriteLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getAllConfigsFormatString lock error");
        }
        return null;
    }

    public String getDataVersionJson() {
        return this.dataVersion.toJson();
    }

    public Properties getAllConfigs() {
        try {
            readWriteLock.readLock().lockInterruptibly();
            try {
                return this.allConfigs;
            } finally {
                readWriteLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getAllConfigs lock error");
        }
        return null;
    }

    private String getAllConfigsInternal() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Object configObject : this.configObjectList) {
            Properties properties = MixAll.object2Properties(configObject);
            merge(properties, this.allConfigs);
        }
        stringBuilder.append(MixAll.properties2String(this.allConfigs, true));
        return stringBuilder.toString();
    }

    private void merge(Properties from, Properties to) {
        for (Entry<Object, Object> next : from.entrySet()) {
            Object fromObj = next.getValue(), toObj = to.get(next.getKey());
            if (toObj != null && !toObj.equals(fromObj)) {
                log.info("Replace, key: {}, value: {} -> {}", next.getKey(), toObj, fromObj);
            }
            to.put(next.getKey(), fromObj);
        }
    }

    private void mergeIfExist(Properties from, Properties to) {
        for (Entry<Object, Object> next : from.entrySet()) {
            if (!to.containsKey(next.getKey())) {
                continue;
            }
            Object fromObj = next.getValue(), toObj = to.get(next.getKey());
            if (toObj != null && !toObj.equals(fromObj)) {
                log.info("Replace, key: {}, value: {} -> {}", next.getKey(), toObj, fromObj);
            }
            to.put(next.getKey(), fromObj);
        }
    }

}