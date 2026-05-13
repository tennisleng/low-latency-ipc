package com.tennisleng.ipc;

import java.nio.MappedByteBuffer;

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
    private int offset;      // start of this message slot in the buffer
    private int slotSize;    // total slot size

    /**
     * Wrap this view around a specific slot in the ring buffer.
     * No data is copied — we just save the coordinates.
     *
     * @param buffer  the underlying memory-mapped buffer
     * @param offset  byte offset where this message slot starts
     * @param slotSize  total size of the slot
     */
    public void wrap(MappedByteBuffer buffer, int offset, int slotSize) {
        this.buffer = buffer;
        this.offset = offset;
        this.slotSize = slotSize;
    }

    /**
     * Get the length of the payload in bytes.
     * Reads directly from the memory-mapped region — no copy.
     */
    public long getPayloadLength() {
        return buffer.getLong(offset);
    }

    /**
     * Read a single byte from the payload at a given index.
     *
     * @param index position within the payload (0-based)
     * @return the byte at that position
     */
    public byte getPayloadByte(int index) {
        return buffer.get(offset + Long.BYTES + index);
    }

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
     */
    public void copyPayloadTo(byte[] dest) {
        int len = (int) getPayloadLength();
        for (int i = 0; i < len; i++) {
            dest[i] = buffer.get(offset + Long.BYTES + i);
        }
    }

    /**
     * Convenience: read payload as a UTF-8 string.
     * WARNING: This creates a new String object (allocation!).
     * Use only for debugging, NOT in your hot path.
     */
    public String getPayloadAsString() {
        int len = (int) getPayloadLength();
        byte[] data = new byte[len];
        copyPayloadTo(data);
        return new String(data, java.nio.charset.StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
        return "MessageView{offset=" + offset + ", payloadLen=" + getPayloadLength() + "}";
    }
}
