package org.apache.rocketmq.common.metrics;


import lombok.Getter;

@Getter
public enum MetricsExporterType {

    DISABLE(0), OTLP_GRPC(1), PROM(2), LOG(3);

    private final int value;

    MetricsExporterType(int value) {
        this.value = value;
    }

    public static MetricsExporterType valueOf(int value) {
        switch (value) {
            case 1:
                return OTLP_GRPC;
            case 2:
                return PROM;
            case 3:
                return LOG;
            default:
                return DISABLE;
        }
    }

    public boolean isEnable() {
        return this.value > 0;
    }

}