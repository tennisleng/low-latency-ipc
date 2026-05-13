package com.tennisleng.ipc;

import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Flyweight pattern — a reusable "view" over a message in the ring buffer.
 *
 * ┌─────────────────────────────────────────────────────────────────┐
 * │  THE ZERO-ALLOCATION TRICK                                      │
 * │                                                                 │
 * │  In a typical Java queue:                                       │
 * │    Message msg = queue.poll();  // ← creates a NEW object       │
 * │                                 // ← GC must clean it up later  │
 * │                                                                 │
 * │  With Flyweight:                                                │
 * │    view.wrap(buffer, offset);   // ← reuses the SAME object    │
 * │                                 // ← GC has nothing to do!      │
 * │                                                                 │
 * │  In your hot loop:                                              │
 * │    MessageView view = new MessageView();  // ONE allocation     │
 * │    while (true) {                                               │
 * │        if (ringBuffer.read(view)) {       // ZERO allocations!  │
 * │            process(view);                                       │
 * │        }                                                        │
 * │    }                                                            │
 * └─────────────────────────────────────────────────────────────────┘
 *
 * This is the exact same pattern used by:
 * - SBE (Simple Binary Encoding) in real trading systems
 * - Aeron messaging framework
 * - Protobuf's zero-copy API
 *
 * The key insight: we never copy the data out of the buffer.
 * We just point to where it already lives in memory.
 */
public class MessageView {

    private MappedByteBuffer buffer;
    private int offset;          // start of this message slot in the buffer
    private int slotSize;        // total slot size
    private int maxPayloadSize;  // max payload capacity per slot
    private int payloadOffset;   // offset + LENGTH_FIELD_SIZE (cached)

    /**
     * Wrap this view around a specific slot in the ring buffer.
     * No data is copied — we just save the coordinates.
     *
     * @param buffer         the underlying memory-mapped buffer
     * @param offset         byte offset where this message slot starts
     * @param slotSize       total size of the slot
     * @param maxPayloadSize maximum payload bytes this slot can hold
     */
    public void wrap(MappedByteBuffer buffer, int offset, int slotSize, int maxPayloadSize) {
        this.buffer = buffer;
        this.offset = offset;
        this.slotSize = slotSize;
        this.maxPayloadSize = maxPayloadSize;
        this.payloadOffset = offset + Long.BYTES; // skip the length field
    }

    // ── Length ───────────────────────────────────────────────────────────

    /**
     * Get the length of the payload in bytes.
     * Reads directly from the memory-mapped region — no copy.
     */
    public int getPayloadLength() {
        return (int) buffer.getLong(offset);
    }

    // ── Raw byte access ─────────────────────────────────────────────────

    /**
     * Read a single byte from the payload at a given index.
     *
     * @param index position within the payload (0-based)
     * @return the byte at that position
     */
    public byte getByte(int index) {
        return buffer.get(payloadOffset + index);
    }

    // ── Typed accessors — read structured data directly from the buffer ─

    /**
     * Read a long (8 bytes) from the payload at a given byte offset.
     * Useful for structured messages where you know the layout:
     *
     *   view.putLong(0, timestamp);    // producer side
     *   long ts = view.getLong(0);     // consumer side
     */
    public long getLong(int payloadIndex) {
        return buffer.getLong(payloadOffset + payloadIndex);
    }

    /**
     * Read an int (4 bytes) from the payload at a given byte offset.
     */
    public int getInt(int payloadIndex) {
        return buffer.getInt(payloadOffset + payloadIndex);
    }

    /**
     * Read a short (2 bytes) from the payload at a given byte offset.
     */
    public short getShort(int payloadIndex) {
        return buffer.getShort(payloadOffset + payloadIndex);
    }

    /**
     * Read a double (8 bytes) from the payload at a given byte offset.
     */
    public double getDouble(int payloadIndex) {
        return buffer.getDouble(payloadOffset + payloadIndex);
    }

    // ── Bulk copy ───────────────────────────────────────────────────────

    /**
     * Copy the payload into a destination byte array.
     *
     * NOTE: This DOES allocate if you create a new byte[] inside your loop.
     * For true zero-alloc, pre-allocate the destination array outside the loop:
     *
     *   byte[] reusableBuffer = new byte[MAX_SIZE];  // allocate ONCE
     *   while (true) {
     *       if (ring.read(view)) {
     *           view.copyPayloadTo(reusableBuffer);  // zero alloc
     *       }
     *   }
     *
     * @param dest   destination array (must be >= getPayloadLength())
     * @return number of bytes copied
     */
    public int copyPayloadTo(byte[] dest) {
        int len = getPayloadLength();
        for (int i = 0; i < len; i++) {
            dest[i] = buffer.get(payloadOffset + i);
        }
        return len;
    }

    /**
     * Copy the payload into a destination array at a specific offset.
     *
     * @param dest      destination array
     * @param destOffset offset within dest to start writing
     * @return number of bytes copied
     */
    public int copyPayloadTo(byte[] dest, int destOffset) {
        int len = getPayloadLength();
        for (int i = 0; i < len; i++) {
            dest[destOffset + i] = buffer.get(payloadOffset + i);
        }
        return len;
    }

    // ── Convenience (allocating — debug only) ───────────────────────────

    /**
     * Read payload as a UTF-8 string.
     * WARNING: This creates a new String object (allocation!).
     * Use only for debugging / logging, NOT in your hot path.
     */
    public String getPayloadAsString() {
        int len = getPayloadLength();
        byte[] data = new byte[len];
        for (int i = 0; i < len; i++) {
            data[i] = buffer.get(payloadOffset + i);
        }
        return new String(data, StandardCharsets.UTF_8);
    }

    /**
     * Copy the full payload into a new byte array.
     * WARNING: Allocates! Use copyPayloadTo(byte[]) in the hot path instead.
     */
    public byte[] getPayloadCopy() {
        int len = getPayloadLength();
        byte[] data = new byte[len];
        for (int i = 0; i < len; i++) {
            data[i] = buffer.get(payloadOffset + i);
        }
        return data;
    }

    // ── Introspection ───────────────────────────────────────────────────

    /** The byte offset of this slot within the mapped file. */
    public int getSlotOffset() {
        return offset;
    }

    /** Max payload capacity of this slot. */
    public int getMaxPayloadSize() {
        return maxPayloadSize;
    }

    /** Whether this view has been wrapped (pointed at a valid slot). */
    public boolean isValid() {
        return buffer != null;
    }

    @Override
    public String toString() {
        if (buffer == null) {
            return "MessageView{unwrapped}";
        }
        return "MessageView{offset=" + offset
                + ", payloadLen=" + getPayloadLength()
                + ", maxPayload=" + maxPayloadSize + "}";
    }
}
