package org.apache.rocketmq.client.trace;

import lombok.Getter;
import lombok.Setter;

import java.util.HashSet;
import java.util.Set;

@Getter
@Setter
public class TraceTransferBean {

    private String transData;

    private Set<String> transKey = new HashSet<>();

}