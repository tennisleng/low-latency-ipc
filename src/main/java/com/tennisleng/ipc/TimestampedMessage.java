package com.tennisleng.ipc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Embeds a nanoTime timestamp at the start of each message so the consumer
 * can measure real end-to-end latency (not just how long read() took).
 *
 * Format: [8B timestamp][user payload bytes...]
 */
public final class TimestampedMessage {

    public static final int TIMESTAMP_SIZE = Long.BYTES;

    private TimestampedMessage() {}

    /** Writes timestamp + payload into dest. Flips it so it's ready for ring.write(). */
    public static ByteBuffer encode(byte[] userPayload, ByteBuffer dest) {
        dest.clear();
        dest.order(ByteOrder.nativeOrder());
        dest.putLong(System.nanoTime());
        dest.put(userPayload);
        dest.flip();
        return dest;
    }

    /** Timestamp only, no user payload. For pure latency benchmarking. */
    public static ByteBuffer encodeTimestampOnly(ByteBuffer dest) {
        dest.clear();
        dest.order(ByteOrder.nativeOrder());
        dest.putLong(System.nanoTime());
        dest.flip();
        return dest;
    }

    public static long decodeSendTimestamp(MessageView view) {
        return view.getLong(0);
    }

    /** Returns nanos since the message was sent. */
    public static long decodeLatency(MessageView view) {
        return System.nanoTime() - view.getLong(0);
    }

    /** Extracts just the user part (after the timestamp). Allocates — debug only. */
    public static String decodeUserPayloadAsString(MessageView view) {
        int userLen = view.getPayloadLength() - TIMESTAMP_SIZE;
        if (userLen <= 0) return "";
        byte[] data = new byte[userLen];
        for (int i = 0; i < userLen; i++) {
            data[i] = view.getByte(TIMESTAMP_SIZE + i);
        }
        return new String(data, StandardCharsets.UTF_8);
    }
}
