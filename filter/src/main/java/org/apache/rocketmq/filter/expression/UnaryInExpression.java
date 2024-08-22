package org.apache.rocketmq.filter.expression;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.filter.constant.UnaryType;

import java.util.Collection;

@Getter
@Setter
abstract public class UnaryInExpression extends UnaryExpression implements BooleanExpression {

    private boolean not;

    private Collection inList;

    public UnaryInExpression(Expression left, UnaryType unaryType, Collection inList, boolean not) {
        super(left, unaryType);
        this.setInList(inList);
        this.setNot(not);

    }

    public boolean matches(EvaluationContext context) throws Exception {
        Object object = evaluate(context);
        return object == Boolean.TRUE;
    }

}