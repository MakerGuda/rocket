package org.apache.rocketmq.filter.expression;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.filter.constant.UnaryType;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;


@Getter
@Setter
public abstract class UnaryExpression implements Expression {

    private static final BigDecimal BD_LONG_MIN_VALUE = BigDecimal.valueOf(Long.MIN_VALUE);

    public UnaryType unaryType;

    protected Expression right;

    public UnaryExpression(Expression left) {
        this.right = left;
    }

    public UnaryExpression(Expression left, UnaryType unaryType) {
        this.setUnaryType(unaryType);
        this.right = left;
    }

    public static Expression createNegate(Expression left) {
        return new UnaryExpression(left, UnaryType.NEGATE) {
            @Override
            public Object evaluate(EvaluationContext context) throws Exception {
                Object rvalue = right.evaluate(context);
                if (rvalue == null) {
                    return null;
                }
                if (rvalue instanceof Number) {
                    return negate((Number) rvalue);
                }
                return null;
            }
        };
    }

    public static BooleanExpression createInExpression(PropertyExpression right, List<Object> elements, final boolean not) {
        Collection<Object> t;
        if (elements.isEmpty()) {
            t = null;
        } else if (elements.size() < 5) {
            t = elements;
        } else {
            t = new HashSet<>(elements);
        }
        final Collection<Object> inList = t;
        return new UnaryInExpression(right, UnaryType.IN, inList, not) {
            @Override
            public Object evaluate(EvaluationContext context) throws Exception {
                Object rvalue = right.evaluate(context);
                if (rvalue == null) {
                    return null;
                }
                if (rvalue.getClass() != String.class) {
                    return null;
                }
                if ((inList != null && inList.contains(rvalue)) ^ not) {
                    return Boolean.TRUE;
                } else {
                    return Boolean.FALSE;
                }
            }

            @Override
            public String toString() {
                StringBuilder answer = new StringBuilder();
                answer.append(right);
                answer.append(" ");
                answer.append(" ( ");
                int count = 0;
                assert inList != null;
                for (Object o : inList) {
                    if (count != 0) {
                        answer.append(", ");
                    }
                    answer.append(o);
                    count++;
                }
                answer.append(" )");
                return answer.toString();
            }
        };
    }

    public static BooleanExpression createNOT(BooleanExpression left) {
        return new BooleanUnaryExpression(left, UnaryType.NOT) {
            @Override
            public Object evaluate(EvaluationContext context) throws Exception {
                Boolean lvalue = (Boolean) right.evaluate(context);
                if (lvalue == null) {
                    return null;
                }
                return lvalue ? Boolean.FALSE : Boolean.TRUE;
            }
        };
    }

    public static BooleanExpression createBooleanCast(Expression left) {
        return new BooleanUnaryExpression(left, UnaryType.BOOLEANCAST) {
            @Override
            public Object evaluate(EvaluationContext context) throws Exception {
                Object rvalue = right.evaluate(context);
                if (rvalue == null) {
                    return null;
                }
                if (!rvalue.getClass().equals(Boolean.class)) {
                    return Boolean.FALSE;
                }
                return (Boolean) rvalue ? Boolean.TRUE : Boolean.FALSE;
            }

            @Override
            public String toString() {
                return right.toString();
            }

        };
    }

    private static Number negate(Number left) {
        Class<?> clazz = left.getClass();
        if (clazz == Integer.class) {
            return -left.intValue();
        } else if (clazz == Long.class) {
            return -left.longValue();
        } else if (clazz == Float.class) {
            return -left.floatValue();
        } else if (clazz == Double.class) {
            return -left.doubleValue();
        } else if (clazz == BigDecimal.class) {
            BigDecimal bd = (BigDecimal) left;
            bd = bd.negate();
            if (BD_LONG_MIN_VALUE.compareTo(bd) == 0) {
                return Long.MIN_VALUE;
            }
            return bd;
        } else {
            throw new RuntimeException("Don't know how to negate: " + left);
        }
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !this.getClass().equals(o.getClass())) {
            return false;
        }
        return toString().equals(o.toString());
    }

    abstract static class BooleanUnaryExpression extends UnaryExpression implements BooleanExpression {

        public BooleanUnaryExpression(Expression left, UnaryType unaryType) {
            super(left, unaryType);
        }

        @Override
        public boolean matches(EvaluationContext context) throws Exception {
            Object object = evaluate(context);
            return object == Boolean.TRUE;
        }
    }

}