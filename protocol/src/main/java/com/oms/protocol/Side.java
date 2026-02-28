package com.oms.protocol;

public enum Side {
    BUY((byte) 1), SELL((byte) 2);

    public final byte code;
    Side(byte code) { this.code = code; }

    public static Side fromCode(byte code) {
        return code == 1 ? BUY : SELL;
    }
}
