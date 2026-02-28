package com.oms.protocol;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;

/**
 * Binary wire protocol codec.
 *
 * TCP Frame layout (little-endian):
 *   [2 bytes: total frame length][1 byte: MsgType][N bytes: payload]
 *
 * NewOrderSingle payload (47 bytes):
 *   sessionId(4) clientId(8) clientSeqNo(8) instrumentId(4)
 *   side(1) tif(1) price(8) qty(8) recvTsNanos(8) reserved(3 pad)
 *   = 47 bytes + 3 header = 50 total
 *
 * All fields little-endian unless noted.
 *
 * Internal Aeron messages add internalOrderId(8) prepended → same layout otherwise.
 * Outbound messages (Ack, Fill, Reject) have compact layouts defined below.
 */
public final class Messages {

    // ---- Frame constants ----
    public static final int FRAME_LENGTH_OFFSET = 0;
    public static final int FRAME_TYPE_OFFSET   = 2;
    public static final int FRAME_HEADER_SIZE   = 3;

    // ---- NewOrderSingle payload offsets (relative to payload start) ----
    public static final int NOS_SESSION_ID_OFFSET     = 0;
    public static final int NOS_CLIENT_ID_OFFSET      = 4;
    public static final int NOS_CLIENT_SEQNO_OFFSET   = 12;
    public static final int NOS_INSTRUMENT_ID_OFFSET  = 20;
    public static final int NOS_SIDE_OFFSET           = 24;
    public static final int NOS_TIF_OFFSET            = 25;
    public static final int NOS_PRICE_OFFSET          = 26; // fixed-point: price * 1_000_000 (int64)
    public static final int NOS_QTY_OFFSET            = 34;
    public static final int NOS_RECV_TS_OFFSET        = 42;
    public static final int NOS_PAYLOAD_SIZE          = 50;

    // ---- Internal Aeron NewOrderSingle: NOS + internalOrderId prepended ----
    public static final int INOS_INTERNAL_ID_OFFSET   = 0;
    public static final int INOS_SESSION_ID_OFFSET    = 8;
    public static final int INOS_CLIENT_ID_OFFSET     = 16;
    public static final int INOS_CLIENT_SEQNO_OFFSET  = 24;
    public static final int INOS_INSTRUMENT_ID_OFFSET = 32;
    public static final int INOS_SIDE_OFFSET          = 36;
    public static final int INOS_TIF_OFFSET           = 37;
    public static final int INOS_PRICE_OFFSET         = 38;
    public static final int INOS_QTY_OFFSET           = 46;
    public static final int INOS_RECV_TS_OFFSET       = 54;
    public static final int INOS_PAYLOAD_SIZE         = 62;

    // ---- CancelRequest payload ----
    public static final int CANCEL_SESSION_ID_OFFSET     = 0;
    public static final int CANCEL_CLIENT_SEQNO_OFFSET   = 4;
    public static final int CANCEL_INTERNAL_ID_OFFSET    = 12;
    public static final int CANCEL_INSTRUMENT_ID_OFFSET  = 20;
    public static final int CANCEL_PAYLOAD_SIZE          = 24;

    // ---- Ack payload (outbound) ----
    public static final int ACK_INTERNAL_ID_OFFSET    = 0;
    public static final int ACK_CLIENT_SEQNO_OFFSET   = 8;
    public static final int ACK_SESSION_ID_OFFSET     = 16;
    public static final int ACK_INSTRUMENT_ID_OFFSET  = 20;
    public static final int ACK_TS_OFFSET             = 24;
    public static final int ACK_PAYLOAD_SIZE          = 32;

    // ---- Reject payload (outbound) ----
    public static final int REJECT_SESSION_ID_OFFSET   = 0;
    public static final int REJECT_CLIENT_SEQNO_OFFSET = 4;
    public static final int REJECT_REASON_OFFSET       = 12;
    public static final int REJECT_PAYLOAD_SIZE        = 13;

    // ---- Fill payload (outbound) ----
    public static final int FILL_INTERNAL_ID_OFFSET   = 0;
    public static final int FILL_SESSION_ID_OFFSET    = 8;
    public static final int FILL_INSTRUMENT_ID_OFFSET = 12;
    public static final int FILL_SIDE_OFFSET          = 16;
    public static final int FILL_FILL_PRICE_OFFSET    = 17;
    public static final int FILL_FILL_QTY_OFFSET      = 25;
    public static final int FILL_LEAVES_QTY_OFFSET    = 33;
    public static final int FILL_TS_OFFSET            = 41;
    public static final int FILL_PAYLOAD_SIZE         = 49;

    // ---- CancelAck payload ----
    public static final int CACK_INTERNAL_ID_OFFSET   = 0;
    public static final int CACK_SESSION_ID_OFFSET    = 8;
    public static final int CACK_PAYLOAD_SIZE         = 12;

    // Price scaling: prices stored as long = price * PRICE_SCALE
    public static final long PRICE_SCALE = 1_000_000L;

    // ---- Aeron payload-only encoders (no TCP frame header) ----
    // Layout in Aeron: [1 byte MsgType][payload bytes]

    public static int encodeAeronAck(MutableDirectBuffer buf, int offset,
                                      long internalOrderId, long clientSeqNo,
                                      int sessionId, int instrumentId, long tsNanos) {
        buf.putByte(offset, MsgType.ACK.code);
        int p = offset + 1;
        buf.putLong(p + ACK_INTERNAL_ID_OFFSET, internalOrderId, LE);
        buf.putLong(p + ACK_CLIENT_SEQNO_OFFSET, clientSeqNo, LE);
        buf.putInt(p + ACK_SESSION_ID_OFFSET, sessionId, LE);
        buf.putInt(p + ACK_INSTRUMENT_ID_OFFSET, instrumentId, LE);
        buf.putLong(p + ACK_TS_OFFSET, tsNanos, LE);
        return 1 + ACK_PAYLOAD_SIZE;
    }

    public static int encodeAeronFill(MutableDirectBuffer buf, int offset,
                                       long internalOrderId, int sessionId, int instrumentId,
                                       Side side, long fillPrice, long fillQty, long leavesQty, long tsNanos) {
        buf.putByte(offset, MsgType.FILL.code);
        int p = offset + 1;
        buf.putLong(p + FILL_INTERNAL_ID_OFFSET, internalOrderId, LE);
        buf.putInt(p + FILL_SESSION_ID_OFFSET, sessionId, LE);
        buf.putInt(p + FILL_INSTRUMENT_ID_OFFSET, instrumentId, LE);
        buf.putByte(p + FILL_SIDE_OFFSET, side.code);
        buf.putLong(p + FILL_FILL_PRICE_OFFSET, fillPrice, LE);
        buf.putLong(p + FILL_FILL_QTY_OFFSET, fillQty, LE);
        buf.putLong(p + FILL_LEAVES_QTY_OFFSET, leavesQty, LE);
        buf.putLong(p + FILL_TS_OFFSET, tsNanos, LE);
        return 1 + FILL_PAYLOAD_SIZE;
    }

    public static int encodeAeronReject(MutableDirectBuffer buf, int offset,
                                         int sessionId, long clientSeqNo, RejectReason reason) {
        buf.putByte(offset, MsgType.REJECT.code);
        int p = offset + 1;
        buf.putInt(p + REJECT_SESSION_ID_OFFSET, sessionId, LE);
        buf.putLong(p + REJECT_CLIENT_SEQNO_OFFSET, clientSeqNo, LE);
        buf.putByte(p + REJECT_REASON_OFFSET, reason.code);
        return 1 + REJECT_PAYLOAD_SIZE;
    }

    public static int encodeAeronCancelAck(MutableDirectBuffer buf, int offset,
                                            long internalOrderId, int sessionId) {
        buf.putByte(offset, MsgType.CANCEL_ACK.code);
        int p = offset + 1;
        buf.putLong(p + CACK_INTERNAL_ID_OFFSET, internalOrderId, LE);
        buf.putInt(p + CACK_SESSION_ID_OFFSET, sessionId, LE);
        return 1 + CACK_PAYLOAD_SIZE;
    }

    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;

    // ---- Frame helpers ----

    public static void writeFrameHeader(MutableDirectBuffer buf, int offset, MsgType type, int payloadSize) {
        // Length = bytes following the 2-byte length field = 1 (type) + payloadSize
        buf.putShort(offset + FRAME_LENGTH_OFFSET, (short)(1 + payloadSize), LE);
        buf.putByte(offset + FRAME_TYPE_OFFSET, type.code);
    }

    public static int readFrameLength(DirectBuffer buf, int offset) {
        return buf.getShort(offset + FRAME_LENGTH_OFFSET, LE) & 0xFFFF;
    }

    public static MsgType readFrameType(DirectBuffer buf, int offset) {
        return MsgType.fromCode(buf.getByte(offset + FRAME_TYPE_OFFSET));
    }

    // ---- NewOrderSingle TCP encode (client -> gateway) ----

    public static int encodeNewOrder(MutableDirectBuffer buf, int offset,
                                     int sessionId, long clientId, long clientSeqNo,
                                     int instrumentId, Side side, TimeInForce tif,
                                     long price, long qty, long recvTsNanos) {
        writeFrameHeader(buf, offset, MsgType.NEW_ORDER, NOS_PAYLOAD_SIZE);
        int p = offset + FRAME_HEADER_SIZE;
        buf.putInt(p + NOS_SESSION_ID_OFFSET, sessionId, LE);
        buf.putLong(p + NOS_CLIENT_ID_OFFSET, clientId, LE);
        buf.putLong(p + NOS_CLIENT_SEQNO_OFFSET, clientSeqNo, LE);
        buf.putInt(p + NOS_INSTRUMENT_ID_OFFSET, instrumentId, LE);
        buf.putByte(p + NOS_SIDE_OFFSET, side.code);
        buf.putByte(p + NOS_TIF_OFFSET, tif.code);
        buf.putLong(p + NOS_PRICE_OFFSET, price, LE);
        buf.putLong(p + NOS_QTY_OFFSET, qty, LE);
        buf.putLong(p + NOS_RECV_TS_OFFSET, recvTsNanos, LE);
        return FRAME_HEADER_SIZE + NOS_PAYLOAD_SIZE;
    }

    // ---- Internal Aeron NewOrderSingle encode (gateway -> engine) ----

    public static int encodeInternalNewOrder(MutableDirectBuffer buf, int offset,
                                              long internalOrderId,
                                              int sessionId, long clientId, long clientSeqNo,
                                              int instrumentId, Side side, TimeInForce tif,
                                              long price, long qty, long recvTsNanos) {
        // No frame header for internal Aeron messages; Aeron handles framing
        buf.putLong(offset + INOS_INTERNAL_ID_OFFSET, internalOrderId, LE);
        buf.putInt(offset + INOS_SESSION_ID_OFFSET, sessionId, LE);
        buf.putLong(offset + INOS_CLIENT_ID_OFFSET, clientId, LE);
        buf.putLong(offset + INOS_CLIENT_SEQNO_OFFSET, clientSeqNo, LE);
        buf.putInt(offset + INOS_INSTRUMENT_ID_OFFSET, instrumentId, LE);
        buf.putByte(offset + INOS_SIDE_OFFSET, side.code);
        buf.putByte(offset + INOS_TIF_OFFSET, tif.code);
        buf.putLong(offset + INOS_PRICE_OFFSET, price, LE);
        buf.putLong(offset + INOS_QTY_OFFSET, qty, LE);
        buf.putLong(offset + INOS_RECV_TS_OFFSET, recvTsNanos, LE);
        return INOS_PAYLOAD_SIZE;
    }

    // ---- Internal Aeron CancelRequest ----

    public static int encodeInternalCancel(MutableDirectBuffer buf, int offset,
                                            int sessionId, long clientSeqNo,
                                            long internalOrderId, int instrumentId) {
        buf.putInt(offset + CANCEL_SESSION_ID_OFFSET, sessionId, LE);
        buf.putLong(offset + CANCEL_CLIENT_SEQNO_OFFSET, clientSeqNo, LE);
        buf.putLong(offset + CANCEL_INTERNAL_ID_OFFSET, internalOrderId, LE);
        buf.putInt(offset + CANCEL_INSTRUMENT_ID_OFFSET, instrumentId, LE);
        return CANCEL_PAYLOAD_SIZE;
    }

    // ---- Outbound Ack encode ----

    public static int encodeAck(MutableDirectBuffer buf, int offset,
                                 long internalOrderId, long clientSeqNo,
                                 int sessionId, int instrumentId, long tsNanos) {
        writeFrameHeader(buf, offset, MsgType.ACK, ACK_PAYLOAD_SIZE);
        int p = offset + FRAME_HEADER_SIZE;
        buf.putLong(p + ACK_INTERNAL_ID_OFFSET, internalOrderId, LE);
        buf.putLong(p + ACK_CLIENT_SEQNO_OFFSET, clientSeqNo, LE);
        buf.putInt(p + ACK_SESSION_ID_OFFSET, sessionId, LE);
        buf.putInt(p + ACK_INSTRUMENT_ID_OFFSET, instrumentId, LE);
        buf.putLong(p + ACK_TS_OFFSET, tsNanos, LE);
        return FRAME_HEADER_SIZE + ACK_PAYLOAD_SIZE;
    }

    // ---- Outbound Reject encode ----

    public static int encodeReject(MutableDirectBuffer buf, int offset,
                                    int sessionId, long clientSeqNo, RejectReason reason) {
        writeFrameHeader(buf, offset, MsgType.REJECT, REJECT_PAYLOAD_SIZE);
        int p = offset + FRAME_HEADER_SIZE;
        buf.putInt(p + REJECT_SESSION_ID_OFFSET, sessionId, LE);
        buf.putLong(p + REJECT_CLIENT_SEQNO_OFFSET, clientSeqNo, LE);
        buf.putByte(p + REJECT_REASON_OFFSET, reason.code);
        return FRAME_HEADER_SIZE + REJECT_PAYLOAD_SIZE;
    }

    // ---- Outbound Fill encode ----

    public static int encodeFill(MutableDirectBuffer buf, int offset,
                                  long internalOrderId, int sessionId, int instrumentId,
                                  Side side, long fillPrice, long fillQty, long leavesQty, long tsNanos) {
        writeFrameHeader(buf, offset, MsgType.FILL, FILL_PAYLOAD_SIZE);
        int p = offset + FRAME_HEADER_SIZE;
        buf.putLong(p + FILL_INTERNAL_ID_OFFSET, internalOrderId, LE);
        buf.putInt(p + FILL_SESSION_ID_OFFSET, sessionId, LE);
        buf.putInt(p + FILL_INSTRUMENT_ID_OFFSET, instrumentId, LE);
        buf.putByte(p + FILL_SIDE_OFFSET, side.code);
        buf.putLong(p + FILL_FILL_PRICE_OFFSET, fillPrice, LE);
        buf.putLong(p + FILL_FILL_QTY_OFFSET, fillQty, LE);
        buf.putLong(p + FILL_LEAVES_QTY_OFFSET, leavesQty, LE);
        buf.putLong(p + FILL_TS_OFFSET, tsNanos, LE);
        return FRAME_HEADER_SIZE + FILL_PAYLOAD_SIZE;
    }

    // ---- Outbound CancelAck encode ----

    public static int encodeCancelAck(MutableDirectBuffer buf, int offset,
                                       long internalOrderId, int sessionId) {
        writeFrameHeader(buf, offset, MsgType.CANCEL_ACK, CACK_PAYLOAD_SIZE);
        int p = offset + FRAME_HEADER_SIZE;
        buf.putLong(p + CACK_INTERNAL_ID_OFFSET, internalOrderId, LE);
        buf.putInt(p + CACK_SESSION_ID_OFFSET, sessionId, LE);
        return FRAME_HEADER_SIZE + CACK_PAYLOAD_SIZE;
    }

    // ---- Logon encode (client -> gateway) ----
    // Logon payload: sessionId(4) clientId(8) = 12 bytes
    public static final int LOGON_SESSION_ID_OFFSET = 0;
    public static final int LOGON_CLIENT_ID_OFFSET  = 4;
    public static final int LOGON_PAYLOAD_SIZE      = 12;

    public static int encodeLogon(MutableDirectBuffer buf, int offset, int sessionId, long clientId) {
        writeFrameHeader(buf, offset, MsgType.LOGON, LOGON_PAYLOAD_SIZE);
        int p = offset + FRAME_HEADER_SIZE;
        buf.putInt(p + LOGON_SESSION_ID_OFFSET, sessionId, LE);
        buf.putLong(p + LOGON_CLIENT_ID_OFFSET, clientId, LE);
        return FRAME_HEADER_SIZE + LOGON_PAYLOAD_SIZE;
    }

    public static int encodeLogonAck(MutableDirectBuffer buf, int offset, int sessionId) {
        writeFrameHeader(buf, offset, MsgType.LOGON_ACK, 4);
        buf.putInt(offset + FRAME_HEADER_SIZE, sessionId, LE);
        return FRAME_HEADER_SIZE + 4;
    }

    private Messages() {}
}
