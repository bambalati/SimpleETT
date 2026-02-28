package com.oms.protocol;

public enum TimeInForce {
    GTC((byte) 1),  // Good Till Cancel
    IOC((byte) 2);  // Immediate Or Cancel

    public final byte code;
    TimeInForce(byte code) { this.code = code; }

    public static TimeInForce fromCode(byte code) {
        return code == 1 ? GTC : IOC;
    }
}
