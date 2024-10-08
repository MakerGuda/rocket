package org.apache.rocketmq.auth.authorization.model;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.auth.authorization.enums.Decision;
import org.apache.rocketmq.auth.authorization.enums.PolicyType;
import org.apache.rocketmq.common.action.Action;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Getter
@Setter
public class Policy {

    private PolicyType policyType;

    private List<PolicyEntry> entries;

    public static Policy of(List<Resource> resources, List<Action> actions, Environment environment, Decision decision) {
        return of(PolicyType.CUSTOM, resources, actions, environment, decision);
    }

    public static Policy of(PolicyType policyType, List<Resource> resources, List<Action> actions, Environment environment, Decision decision) {
        Policy policy = new Policy();
        policy.setPolicyType(policyType);
        List<PolicyEntry> entries = resources.stream().map(resource -> PolicyEntry.of(resource, actions, environment, decision)).collect(Collectors.toList());
        policy.setEntries(entries);
        return policy;
    }

    public static Policy of(PolicyType type, List<PolicyEntry> entries) {
        Policy policy = new Policy();
        policy.setPolicyType(type);
        policy.setEntries(entries);
        return policy;
    }

    public void updateEntry(List<PolicyEntry> newEntries) {
        if (this.entries == null) {
            this.entries = new ArrayList<>();
        }
        newEntries.forEach(newEntry -> {
            PolicyEntry entry = getEntry(newEntry.getResource());
            if (entry == null) {
                this.entries.add(newEntry);
            } else {
                entry.updateEntry(newEntry.getActions(), newEntry.getEnvironment(), newEntry.getDecision());
            }
        });
    }

    public void deleteEntry(Resource resources) {
        PolicyEntry entry = getEntry(resources);
        if (entry != null) {
            this.entries.remove(entry);
        }
    }

    private PolicyEntry getEntry(Resource resource) {
        if (CollectionUtils.isEmpty(this.entries)) {
            return null;
        }
        for (PolicyEntry entry : this.entries) {
            if (Objects.equals(entry.getResource(), resource)) {
                return entry;
            }
        }
        return null;
    }

}