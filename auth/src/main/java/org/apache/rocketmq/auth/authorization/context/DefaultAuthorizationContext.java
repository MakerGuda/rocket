package org.apache.rocketmq.auth.authorization.context;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authorization.model.Resource;
import org.apache.rocketmq.common.action.Action;

import java.util.Collections;
import java.util.List;

@Getter
@Setter
public class DefaultAuthorizationContext extends AuthorizationContext {

    private Subject subject;

    private Resource resource;

    private List<Action> actions;

    private String sourceIp;

    public static DefaultAuthorizationContext of(Subject subject, Resource resource, Action action, String sourceIp) {
        DefaultAuthorizationContext context = new DefaultAuthorizationContext();
        context.setSubject(subject);
        context.setResource(resource);
        context.setActions(Collections.singletonList(action));
        context.setSourceIp(sourceIp);
        return context;
    }

    public static DefaultAuthorizationContext of(Subject subject, Resource resource, List<Action> actions, String sourceIp) {
        DefaultAuthorizationContext context = new DefaultAuthorizationContext();
        context.setSubject(subject);
        context.setResource(resource);
        context.setActions(actions);
        context.setSourceIp(sourceIp);
        return context;
    }

    public String getSubjectKey() {
        return this.subject != null ? this.subject.getSubjectKey() : null;
    }

    public String getResourceKey() {
        return this.resource != null ? this.resource.getResourceKey() : null;
    }

}