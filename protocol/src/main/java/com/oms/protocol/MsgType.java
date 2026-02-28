package com.oms.protocol;

/**
 * Wire message type codes (1 byte).
 *
 * Inbound (client -> gateway): 1-19
 * Outbound (gateway -> client): 20-39
 * Internal (gateway <-> engine over Aeron): same codes; direction implied by stream.
 */
public enum MsgType {
    // Inbound
    LOGON           ((byte) 1),
    NEW_ORDER       ((byte) 2),
    CANCEL_REQUEST  ((byte) 3),
    REPLACE_REQUEST ((byte) 4),

    // Outbound
    LOGON_ACK       ((byte) 20),
    ACK             ((byte) 21),
    REJECT          ((byte) 22),
    FILL            ((byte) 23),
    CANCEL_ACK      ((byte) 24),
    ORDER_UPDATE    ((byte) 25),
    HEARTBEAT       ((byte) 30);

    public final byte code;

    MsgType(byte code) { this.code = code; }

    private static final MsgType[] BY_CODE = new MsgType[256];
    static {
        for (MsgType t : values()) BY_CODE[Byte.toUnsignedInt(t.code)] = t;
    }

    public static MsgType fromCode(byte code) {
        MsgType t = BY_CODE[Byte.toUnsignedInt(code)];
        if (t == null) throw new IllegalArgumentException("Unknown MsgType: " + code);
        return t;
    }
}
