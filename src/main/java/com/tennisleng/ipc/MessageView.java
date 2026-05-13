package com.tennisleng.ipc;

import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Flyweight over a ring buffer slot. Allocated once, reused forever.
 *
 * Instead of returning a new Message object on every read (which the GC
 * eventually has to clean up), the consumer reuses a single MessageView
 * that just points to different offsets in the mmap region.
 *
 * Same idea as SBE decoders in trading systems — you never copy the data,
 * you just move the pointer.
 */
public class MessageView {

    private MappedByteBuffer buffer;
    private int offset;
    private int slotSize;
    private int maxPayloadSize;
    private int payloadOff; // cached: offset + 8

    /**
     * Point this view at a specific slot. No data is copied.
     */
    public void wrap(MappedByteBuffer buffer, int offset, int slotSize, int maxPayloadSize) {
        this.buffer = buffer;
        this.offset = offset;
        this.slotSize = slotSize;
        this.maxPayloadSize = maxPayloadSize;
        this.payloadOff = offset + Long.BYTES;
    }

    public int getPayloadLength() {
        return (int) buffer.getLong(offset);
    }

    // --- single-value reads (no allocation) ---

    public byte getByte(int i)     { return buffer.get(payloadOff + i); }
    public long getLong(int i)     { return buffer.getLong(payloadOff + i); }
    public int getInt(int i)       { return buffer.getInt(payloadOff + i); }
    public short getShort(int i)   { return buffer.getShort(payloadOff + i); }
    public double getDouble(int i) { return buffer.getDouble(payloadOff + i); }

    // --- bulk copy into pre-allocated array (zero alloc if dest is reused) ---

    /** Copies payload into dest. Returns number of bytes copied. */
    public int copyPayloadTo(byte[] dest) {
        return copyPayloadTo(dest, 0);
    }

    public int copyPayloadTo(byte[] dest, int destOffset) {
        int len = getPayloadLength();
        for (int i = 0; i < len; i++) {
            dest[destOffset + i] = buffer.get(payloadOff + i);
        }
        return len;
    }

    // --- convenience methods (these allocate — don't use in the hot path) ---

    /** Allocates a String. Fine for logging, not for the read loop. */
    public String getPayloadAsString() {
        int len = getPayloadLength();
        byte[] data = new byte[len];
        for (int i = 0; i < len; i++) {
            data[i] = buffer.get(payloadOff + i);
        }
        return new String(data, StandardCharsets.UTF_8);
    }

    /** Returns a copy. Same warning — allocates. */
    public byte[] getPayloadCopy() {
        int len = getPayloadLength();
        byte[] data = new byte[len];
        for (int i = 0; i < len; i++) {
            data[i] = buffer.get(payloadOff + i);
        }
        return data;
    }

    public int getSlotOffset()      { return offset; }
    public int getMaxPayloadSize()  { return maxPayloadSize; }
    public boolean isValid()        { return buffer != null; }

    @Override
    public String toString() {
        if (buffer == null) return "MessageView{unwrapped}";
        return "MessageView{off=" + offset + ", len=" + getPayloadLength() + "}";
    }
}
