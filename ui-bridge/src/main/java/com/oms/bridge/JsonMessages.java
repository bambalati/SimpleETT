package com.oms.bridge;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.oms.protocol.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Converts between typed domain events and JSON strings exchanged with the browser.
 *
 * JSON → Binary: parses browser JSON, produces binary OMS message
 * Binary → JSON: parses binary OMS response, produces JSON string for browser
 *
 * Price convention: browser uses decimal (e.g. 189.50), bridge scales to/from long.
 */
public final class JsonMessages {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonMessages() {}

    // ── Browser → Bridge ─────────────────────────────────────────────────────

    public static JsonNode parse(String json) throws Exception {
        return MAPPER.readTree(json);
    }

    public static String msgType(JsonNode node) {
        return node.path("type").asText("");
    }

    // ── Bridge → Browser ─────────────────────────────────────────────────────

    public static String logonAck(int sessionId) {
        return obj("type", "LOGON_ACK", "sessionId", sessionId);
    }

    public static String ack(long internalOrderId, long clientSeqNo, int instrumentId, long tsNanos) {
        Map<String, Object> m = new HashMap<>();
        m.put("type", "ACK");
        m.put("internalOrderId", internalOrderId);
        m.put("clientSeqNo", clientSeqNo);
        m.put("instrumentId", instrumentId);
        m.put("tsNanos", tsNanos);
        return write(m);
    }

    public static String fill(long internalOrderId, int sessionId, int instrumentId,
                               String side, long fillPriceScaled, long fillQty, long leavesQty, long tsNanos) {
        Map<String, Object> m = new HashMap<>();
        m.put("type", "FILL");
        m.put("internalOrderId", internalOrderId);
        m.put("sessionId", sessionId);
        m.put("instrumentId", instrumentId);
        m.put("side", side);
        m.put("fillPrice", fillPriceScaled / (double) Messages.PRICE_SCALE);
        m.put("fillQty", fillQty);
        m.put("leavesQty", leavesQty);
        m.put("tsNanos", tsNanos);
        return write(m);
    }

    public static String reject(int sessionId, long clientSeqNo, String reason) {
        Map<String, Object> m = new HashMap<>();
        m.put("type", "REJECT");
        m.put("sessionId", sessionId);
        m.put("clientSeqNo", clientSeqNo);
        m.put("reason", reason);
        return write(m);
    }

    public static String cancelAck(long internalOrderId, int sessionId) {
        Map<String, Object> m = new HashMap<>();
        m.put("type", "CANCEL_ACK");
        m.put("internalOrderId", internalOrderId);
        m.put("sessionId", sessionId);
        return write(m);
    }

    public static String error(String message) {
        return obj("type", "ERROR", "message", message);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static String obj(Object... kv) {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put((String) kv[i], kv[i + 1]);
        return write(m);
    }

    private static String write(Map<String, Object> m) {
        try {
            return MAPPER.writeValueAsString(m);
        } catch (Exception e) {
            return "{\"type\":\"ERROR\",\"message\":\"serialize failed\"}";
        }
    }
}
