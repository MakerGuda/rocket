package org.apache.rocketmq.filter.expression;


import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PropertyExpression implements Expression {

    private final String name;

    public PropertyExpression(String name) {
        this.name = name;
    }

    @Override
    public Object evaluate(EvaluationContext context) throws Exception {
        return context.get(name);
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !this.getClass().equals(o.getClass())) {
            return false;
        }
        return name.equals(((PropertyExpression) o).name);
    }

}