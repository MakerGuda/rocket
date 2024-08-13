package org.apache.rocketmq.auth.authorization.model;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.auth.authorization.enums.Decision;
import org.apache.rocketmq.common.action.Action;

import java.util.List;
import java.util.stream.Collectors;

@Getter
@Setter
public class PolicyEntry {

    private Resource resource;

    private List<Action> actions;

    private Environment environment;

    private Decision decision;

    public static PolicyEntry of(Resource resource, List<Action> actions, Environment environment, Decision decision) {
        PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setResource(resource);
        policyEntry.setActions(actions);
        policyEntry.setEnvironment(environment);
        policyEntry.setDecision(decision);
        return policyEntry;
    }

    public void updateEntry(List<Action> actions, Environment environment, Decision decision) {
        this.setActions(actions);
        this.setEnvironment(environment);
        this.setDecision(decision);
    }

    public boolean isMatchResource(Resource resource) {
        return this.resource.isMatch(resource);
    }

    public boolean isMatchAction(List<Action> actions) {
        if (CollectionUtils.isEmpty(this.actions)) {
            return false;
        }
        if (actions.contains(Action.ANY)) {
            return true;
        }
        return actions.stream().anyMatch(action -> this.actions.contains(action) || this.actions.contains(Action.ALL));
    }

    public boolean isMatchEnvironment(Environment environment) {
        if (this.environment == null) {
            return true;
        }
        return this.environment.isMatch(environment);
    }

    public String toResourceStr() {
        if (resource == null) {
            return null;
        }
        return resource.getResourceKey();
    }

    public List<String> toActionsStr() {
        if (CollectionUtils.isEmpty(actions)) {
            return null;
        }
        return actions.stream().map(Action::getName).collect(Collectors.toList());
    }

}