package org.apache.rocketmq.store.index;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class QueryOffsetResult {

    private final List<Long> phyOffsets;

    private final long indexLastUpdateTimestamp;

    private final long indexLastUpdatePhyoffset;

    public QueryOffsetResult(List<Long> phyOffsets, long indexLastUpdateTimestamp,
        long indexLastUpdatePhyoffset) {
        this.phyOffsets = phyOffsets;
        this.indexLastUpdateTimestamp = indexLastUpdateTimestamp;
        this.indexLastUpdatePhyoffset = indexLastUpdatePhyoffset;
    }

}