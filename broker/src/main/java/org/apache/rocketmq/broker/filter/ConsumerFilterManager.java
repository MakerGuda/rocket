package org.apache.rocketmq.broker.filter;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.filter.FilterFactory;
import org.apache.rocketmq.filter.util.BloomFilter;
import org.apache.rocketmq.filter.util.BloomFilterData;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Getter
@Setter
public class ConsumerFilterManager extends ConfigManager {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.FILTER_LOGGER_NAME);

    private static final long MS_24_HOUR = 24 * 3600 * 1000;

    /**
     * key: topic
     */
    private ConcurrentMap<String, FilterDataMapByTopic> filterDataByTopic = new ConcurrentHashMap<>(256);

    private transient BrokerController brokerController;

    private transient BloomFilter bloomFilter;

    public ConsumerFilterManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.bloomFilter = BloomFilter.createByFn(brokerController.getBrokerConfig().getMaxErrorRateOfBloomFilter(), brokerController.getBrokerConfig().getExpectConsumerNumUseFilter());
        brokerController.getMessageStoreConfig().setBitMapLengthConsumeQueueExt(this.bloomFilter.getM());
    }

    public static ConsumerFilterData build(final String topic, final String consumerGroup, final String expression, final String type, final long clientVersion) {
        if (ExpressionType.isTagType(type)) {
            return null;
        }
        ConsumerFilterData consumerFilterData = new ConsumerFilterData();
        consumerFilterData.setTopic(topic);
        consumerFilterData.setConsumerGroup(consumerGroup);
        consumerFilterData.setBornTime(System.currentTimeMillis());
        consumerFilterData.setDeadTime(0);
        consumerFilterData.setExpression(expression);
        consumerFilterData.setExpressionType(type);
        consumerFilterData.setClientVersion(clientVersion);
        try {
            consumerFilterData.setCompiledExpression(FilterFactory.INSTANCE.get(type).compile(expression));
        } catch (Throwable e) {
            log.error("parse error: expr={}, topic={}, group={}, error={}", expression, topic, consumerGroup, e.getMessage());
            return null;
        }
        return consumerFilterData;
    }

    public void register(final String consumerGroup, final Collection<SubscriptionData> subList) {
        for (SubscriptionData subscriptionData : subList) {
            register(subscriptionData.getTopic(), consumerGroup, subscriptionData.getSubString(), subscriptionData.getExpressionType(), subscriptionData.getSubVersion());
        }
        Collection<ConsumerFilterData> groupFilterData = getByGroup(consumerGroup);
        for (ConsumerFilterData filterData : groupFilterData) {
            boolean exist = false;
            for (SubscriptionData subscriptionData : subList) {
                if (subscriptionData.getTopic().equals(filterData.getTopic())) {
                    exist = true;
                    break;
                }
            }
            if (!exist && !filterData.isDead()) {
                filterData.setDeadTime(System.currentTimeMillis());
                log.info("Consumer filter changed: {}, make illegal topic dead:{}", consumerGroup, filterData);
            }
        }
    }

    public boolean register(final String topic, final String consumerGroup, final String expression, final String type, final long clientVersion) {
        if (ExpressionType.isTagType(type)) {
            return false;
        }
        if (expression == null || expression.isEmpty()) {
            return false;
        }
        FilterDataMapByTopic filterDataMapByTopic = this.filterDataByTopic.get(topic);
        if (filterDataMapByTopic == null) {
            FilterDataMapByTopic temp = new FilterDataMapByTopic(topic);
            FilterDataMapByTopic prev = this.filterDataByTopic.putIfAbsent(topic, temp);
            filterDataMapByTopic = prev != null ? prev : temp;
        }
        BloomFilterData bloomFilterData = bloomFilter.generate(consumerGroup + "#" + topic);
        return filterDataMapByTopic.register(consumerGroup, expression, type, bloomFilterData, clientVersion);
    }

    public void unRegister(final String consumerGroup) {
        for (Entry<String, FilterDataMapByTopic> entry : filterDataByTopic.entrySet()) {
            entry.getValue().unRegister(consumerGroup);
        }
    }

    public ConsumerFilterData get(final String topic, final String consumerGroup) {
        if (!this.filterDataByTopic.containsKey(topic)) {
            return null;
        }
        if (this.filterDataByTopic.get(topic).getGroupFilterData().isEmpty()) {
            return null;
        }
        return this.filterDataByTopic.get(topic).getGroupFilterData().get(consumerGroup);
    }

    public Collection<ConsumerFilterData> getByGroup(final String consumerGroup) {
        Collection<ConsumerFilterData> ret = new HashSet<>();
        for (FilterDataMapByTopic filterDataMapByTopic : this.filterDataByTopic.values()) {
            for (ConsumerFilterData filterData : filterDataMapByTopic.getGroupFilterData().values()) {
                if (filterData.getConsumerGroup().equals(consumerGroup)) {
                    ret.add(filterData);
                }
            }
        }
        return ret;
    }

    public final Collection<ConsumerFilterData> get(final String topic) {
        if (!this.filterDataByTopic.containsKey(topic)) {
            return null;
        }
        if (this.filterDataByTopic.get(topic).getGroupFilterData().isEmpty()) {
            return null;
        }
        return this.filterDataByTopic.get(topic).getGroupFilterData().values();
    }

    @Override
    public String encode() {
        return encode(false);
    }

    @Override
    public String configFilePath() {
        if (this.brokerController != null) {
            return BrokerPathConfigHelper.getConsumerFilterPath(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
        }
        return BrokerPathConfigHelper.getConsumerFilterPath("./unit_test");
    }

    @Override
    public void decode(final String jsonString) {
        ConsumerFilterManager load = RemotingSerializable.fromJson(jsonString, ConsumerFilterManager.class);
        if (load != null && load.filterDataByTopic != null) {
            boolean bloomChanged = false;
            for (Entry<String, FilterDataMapByTopic> entry : load.filterDataByTopic.entrySet()) {
                FilterDataMapByTopic dataMapByTopic = entry.getValue();
                if (dataMapByTopic == null) {
                    continue;
                }
                for (Entry<String, ConsumerFilterData> groupEntry : dataMapByTopic.getGroupFilterData().entrySet()) {
                    ConsumerFilterData filterData = groupEntry.getValue();
                    if (filterData == null) {
                        continue;
                    }
                    try {
                        filterData.setCompiledExpression(FilterFactory.INSTANCE.get(filterData.getExpressionType()).compile(filterData.getExpression()));
                    } catch (Exception e) {
                        log.error("load filter data error, " + filterData, e);
                    }
                    if (!this.bloomFilter.isValid(filterData.getBloomFilterData())) {
                        bloomChanged = true;
                        log.info("Bloom filter is changed!So ignore all filter data persisted! {}, {}", this.bloomFilter, filterData.getBloomFilterData());
                        break;
                    }
                    log.info("load exist consumer filter data: {}", filterData);
                    if (filterData.getDeadTime() == 0) {
                        long deadTime = System.currentTimeMillis() - 30 * 1000;
                        filterData.setDeadTime(Math.max(deadTime, filterData.getBornTime()));
                    }
                }
            }
            if (!bloomChanged) {
                this.filterDataByTopic = load.filterDataByTopic;
            }
        }
    }

    @Override
    public String encode(final boolean prettyFormat) {
        {
            clean();
        }
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    public void clean() {
        Iterator<Map.Entry<String, FilterDataMapByTopic>> topicIterator = this.filterDataByTopic.entrySet().iterator();
        while (topicIterator.hasNext()) {
            Map.Entry<String, FilterDataMapByTopic> filterDataMapByTopic = topicIterator.next();
            Iterator<Map.Entry<String, ConsumerFilterData>> filterDataIterator = filterDataMapByTopic.getValue().getGroupFilterData().entrySet().iterator();
            while (filterDataIterator.hasNext()) {
                Map.Entry<String, ConsumerFilterData> filterDataByGroup = filterDataIterator.next();
                ConsumerFilterData filterData = filterDataByGroup.getValue();
                if (filterData.howLongAfterDeath() >= (this.brokerController == null ? MS_24_HOUR : this.brokerController.getBrokerConfig().getFilterDataCleanTimeSpan())) {
                    log.info("Remove filter consumer {}, died too long!", filterDataByGroup.getValue());
                    filterDataIterator.remove();
                }
            }
            if (filterDataMapByTopic.getValue().getGroupFilterData().isEmpty()) {
                log.info("Topic has no consumer, remove it! {}", filterDataMapByTopic.getKey());
                topicIterator.remove();
            }
        }
    }

    @Getter
    @Setter
    public static class FilterDataMapByTopic {

        /**
         * key: consumer group
         */
        private ConcurrentMap<String, ConsumerFilterData> groupFilterData = new ConcurrentHashMap<>();

        private String topic;

        public FilterDataMapByTopic(String topic) {
            this.topic = topic;
        }

        public void unRegister(String consumerGroup) {
            if (!this.groupFilterData.containsKey(consumerGroup)) {
                return;
            }
            ConsumerFilterData data = this.groupFilterData.get(consumerGroup);
            if (data == null || data.isDead()) {
                return;
            }
            long now = System.currentTimeMillis();
            log.info("Unregister consumer filter: {}, deadTime: {}", data, now);
            data.setDeadTime(now);
        }

        public boolean register(String consumerGroup, String expression, String type, BloomFilterData bloomFilterData, long clientVersion) {
            ConsumerFilterData old = this.groupFilterData.get(consumerGroup);
            if (old == null) {
                ConsumerFilterData consumerFilterData = build(topic, consumerGroup, expression, type, clientVersion);
                if (consumerFilterData == null) {
                    return false;
                }
                consumerFilterData.setBloomFilterData(bloomFilterData);
                old = this.groupFilterData.putIfAbsent(consumerGroup, consumerFilterData);
                if (old == null) {
                    log.info("New consumer filter registered: {}", consumerFilterData);
                    return true;
                } else {
                    if (clientVersion <= old.getClientVersion()) {
                        if (!type.equals(old.getExpressionType()) || !expression.equals(old.getExpression())) {
                            log.warn("Ignore consumer({} : {}) filter(concurrent), because of version {} <= {}, but maybe info changed!old={}:{}, ignored={}:{}", consumerGroup, topic, clientVersion, old.getClientVersion(), old.getExpressionType(), old.getExpression(), type, expression);
                        }
                        if (clientVersion == old.getClientVersion() && old.isDead()) {
                            reAlive(old);
                            return true;
                        }
                        return false;
                    } else {
                        this.groupFilterData.put(consumerGroup, consumerFilterData);
                        log.info("New consumer filter registered(concurrent): {}, old: {}", consumerFilterData, old);
                        return true;
                    }
                }
            } else {
                if (clientVersion <= old.getClientVersion()) {
                    if (!type.equals(old.getExpressionType()) || !expression.equals(old.getExpression())) {
                        log.info("Ignore consumer({}:{}) filter, because of version {} <= {}, but maybe info changed!old={}:{}, ignored={}:{}", consumerGroup, topic, clientVersion, old.getClientVersion(), old.getExpressionType(), old.getExpression(), type, expression);
                    }
                    if (clientVersion == old.getClientVersion() && old.isDead()) {
                        reAlive(old);
                        return true;
                    }
                    return false;
                }
                boolean change = !old.getExpression().equals(expression) || !old.getExpressionType().equals(type);
                if (old.getBloomFilterData() == null && bloomFilterData != null) {
                    change = true;
                }
                if (old.getBloomFilterData() != null && !old.getBloomFilterData().equals(bloomFilterData)) {
                    change = true;
                }
                if (change) {
                    ConsumerFilterData consumerFilterData = build(topic, consumerGroup, expression, type, clientVersion);
                    if (consumerFilterData == null) {
                        this.groupFilterData.remove(consumerGroup);
                        return false;
                    }
                    consumerFilterData.setBloomFilterData(bloomFilterData);
                    this.groupFilterData.put(consumerGroup, consumerFilterData);
                    log.info("Consumer filter info change, old: {}, new: {}, change: {}", old, consumerFilterData, true);
                    return true;
                } else {
                    old.setClientVersion(clientVersion);
                    if (old.isDead()) {
                        reAlive(old);
                    }
                    return true;
                }
            }
        }

        protected void reAlive(ConsumerFilterData filterData) {
            long oldDeadTime = filterData.getDeadTime();
            filterData.setDeadTime(0);
            log.info("Re alive consumer filter: {}, oldDeadTime: {}", filterData, oldDeadTime);
        }

        public final ConsumerFilterData get(String consumerGroup) {
            return this.groupFilterData.get(consumerGroup);
        }

        public final ConcurrentMap<String, ConsumerFilterData> getGroupFilterData() {
            return this.groupFilterData;
        }

    }

}