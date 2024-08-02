package org.apache.rocketmq.common;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class Pair<T1, T2> implements Serializable {

    private T1 object1;

    private T2 object2;

    public Pair(T1 object1, T2 object2) {
        this.object1 = object1;
        this.object2 = object2;
    }

    public static <T1, T2> Pair<T1, T2> of(T1 object1, T2 object2) {
        return new Pair<>(object1, object2);
    }

}