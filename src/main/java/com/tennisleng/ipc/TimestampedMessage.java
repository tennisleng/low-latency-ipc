package com.tennisleng.ipc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Encodes a timestamp INTO the message payload so the consumer can compute
 * true end-to-end latency (producer-to-consumer), not just "how long did
 * the read() call take."
 *
 * Message format:
 *   [8 bytes: nanoTime timestamp] [remaining bytes: user payload]
 *
 * The producer calls:
 *   TimestampedMessage.encode(userPayload, scratchBuffer);
 *   ring.write(scratchBuffer);
 *
 * The consumer calls:
 *   ring.read(view);
 *   long latencyNanos = TimestampedMessage.decodeLatency(view);
 *
 * This gives you the REAL IPC latency: time from producer.write() to
 * consumer.read(), including any spin-wait time, cache coherence delay,
 * and OS scheduling jitter.
 *
 * Zero-allocation: uses a pre-allocated scratch ByteBuffer.
 */
public final class TimestampedMessage {

    /** Timestamp is always the first 8 bytes of the payload. */
    public static final int TIMESTAMP_SIZE = Long.BYTES;

    private TimestampedMessage() {} // utility class — no instances

    /**
     * Encode a user payload with a leading timestamp.
     *
     * @param userPayload  the actual message data
     * @param dest         pre-allocated scratch buffer (must fit TIMESTAMP_SIZE + userPayload.length)
     * @return the dest buffer, flipped and ready for ring.write(ByteBuffer)
     */
    public static ByteBuffer encode(byte[] userPayload, ByteBuffer dest) {
        dest.clear();
        dest.order(ByteOrder.nativeOrder());
        dest.putLong(System.nanoTime());
        dest.put(userPayload);
        dest.flip();
        return dest;
    }

    /**
     * Encode with just a timestamp (no user payload).
     * Useful for pure latency measurement.
     */
    public static ByteBuffer encodeTimestampOnly(ByteBuffer dest) {
        dest.clear();
        dest.order(ByteOrder.nativeOrder());
        dest.putLong(System.nanoTime());
        dest.flip();
        return dest;
    }

    /**
     * Extract the send timestamp from a received message.
     *
     * @param view the message view from ring.read()
     * @return the nanoTime that was recorded when the producer sent this message
     */
    public static long decodeSendTimestamp(MessageView view) {
        return view.getLong(0);
    }

    /**
     * Compute the one-way latency for a received message.
     *
     * @param view the message view from ring.read()
     * @return latency in nanoseconds (now - sendTimestamp)
     */
    public static long decodeLatency(MessageView view) {
        long sendTime = view.getLong(0);
        return System.nanoTime() - sendTime;
    }

    /**
     * Get the user payload portion (everything after the timestamp).
     * WARNING: allocates — use only for debugging.
     */
    public static String decodeUserPayloadAsString(MessageView view) {
        int totalLen = view.getPayloadLength();
        int userLen = totalLen - TIMESTAMP_SIZE;
        if (userLen <= 0) return "";

        byte[] data = new byte[userLen];
        for (int i = 0; i < userLen; i++) {
            data[i] = view.getByte(TIMESTAMP_SIZE + i);
        }
        return new String(data, java.nio.charset.StandardCharsets.UTF_8);
    }
}
