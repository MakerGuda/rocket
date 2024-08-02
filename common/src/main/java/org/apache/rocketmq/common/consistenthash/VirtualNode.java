package org.apache.rocketmq.common.consistenthash;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class VirtualNode<T extends Node> implements Node {

    final T physicalNode;

    final int replicaIndex;

    public VirtualNode(T physicalNode, int replicaIndex) {
        this.replicaIndex = replicaIndex;
        this.physicalNode = physicalNode;
    }

    @Override
    public String getKey() {
        return physicalNode.getKey() + "-" + replicaIndex;
    }

    public boolean isVirtualNodeOf(T pNode) {
        return physicalNode.getKey().equals(pNode.getKey());
    }

}