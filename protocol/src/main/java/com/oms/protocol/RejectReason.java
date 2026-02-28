package com.oms.protocol;

public enum RejectReason {
    UNKNOWN             ((byte) 0),
    DUPLICATE_SEQNO     ((byte) 1),
    SEQNO_GAP           ((byte) 2),
    SYSTEM_BUSY         ((byte) 3),
    ORDER_NOT_FOUND     ((byte) 4),
    INVALID_PRICE       ((byte) 5),
    INVALID_QTY         ((byte) 6),
    SESSION_NOT_LOGGED_ON((byte) 7);

    public final byte code;
    RejectReason(byte code) { this.code = code; }

    private static final RejectReason[] BY_CODE = new RejectReason[256];
    static {
        for (RejectReason r : values()) BY_CODE[Byte.toUnsignedInt(r.code)] = r;
    }

    public static RejectReason fromCode(byte code) {
        RejectReason r = BY_CODE[Byte.toUnsignedInt(code)];
        return r != null ? r : UNKNOWN;
    }
}
