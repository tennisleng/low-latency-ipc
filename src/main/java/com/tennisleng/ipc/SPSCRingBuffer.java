package com.tennisleng.ipc;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Single Producer Single Consumer (SPSC) Ring Buffer backed by a memory-mapped file.
 *
 * ┌──────────────────────────── FILE LAYOUT ────────────────────────────┐
 * │                                                                     │
 * │  OFFSET 0:   [Producer Sequence (8 bytes)]                          │
 * │  OFFSET 8:   [56 bytes PADDING — false sharing prevention]          │
 * │  OFFSET 64:  [Consumer Sequence (8 bytes)]                          │
 * │  OFFSET 72:  [56 bytes PADDING — false sharing prevention]          │
 * │  OFFSET 128: [Ring Buffer Data Region...]                           │
 * │                                                                     │
 * │  Each message slot:                                                 │
 * │    [8 bytes: payload length] [N bytes: payload] [1 byte: commit]    │
 * │                                                                     │
 * └─────────────────────────────────────────────────────────────────────┘
 *
 * WHY 64-BYTE PADDING?
 * Modern CPUs load memory in 64-byte "cache lines." If the producer's
 * write pointer and consumer's read pointer share the same cache line,
 * the CPUs will constantly invalidate each other's caches ("false sharing"),
 * destroying performance. 56 bytes of dead padding + 8 bytes of the
 * sequence = 64 bytes = 1 full cache line per pointer.
 *
 * @see <a href="https://mechanical-sympathy.blogspot.com/">Mechanical Sympathy</a>
 * @see <a href="https://lmax-exchange.github.io/disruptor/">LMAX Disruptor</a>
 */
public class SPSCRingBuffer implements AutoCloseable {

    // ── Cache Line Constants ────────────────────────────────────────────
    private static final int CACHE_LINE_SIZE = 64;  // bytes — standard x86/ARM
    private static final int SEQUENCE_SIZE   = Long.BYTES; // 8 bytes

    // ── Header Layout ───────────────────────────────────────────────────
    /**
     * Offset where the producer writes its sequence number.
     * The producer sequence represents the NEXT slot to write into.
     */
    static final int PRODUCER_SEQUENCE_OFFSET = 0;

    /**
     * Offset where the consumer writes its sequence number.
     * Separated by exactly one cache line from the producer sequence
     * to prevent false sharing.
     */
    static final int CONSUMER_SEQUENCE_OFFSET = CACHE_LINE_SIZE; // 64

    /**
     * Where the actual ring buffer data begins, after two padded sequences.
     */
    static final int DATA_OFFSET = CACHE_LINE_SIZE * 2; // 128

    // ── Message Slot Layout ─────────────────────────────────────────────
    /**
     * Each message slot has:
     *  - 8 bytes for payload length
     *  - N bytes of payload data
     *  - 1 byte commit flag
     */
    private static final int LENGTH_FIELD_SIZE = Long.BYTES;
    private static final int COMMIT_FLAG_SIZE  = Byte.BYTES;

    // ── VarHandle for Memory Barriers ───────────────────────────────────
    /**
     * VarHandle gives us fine-grained control over memory ordering.
     *
     * Think of it like C++ std::atomic — we can choose:
     *   - Opaque:       no reordering guarantee (fastest, raw read/write)
     *   - Acquire:      LoadLoad + LoadStore barrier (consumer reads)
     *   - Release:      StoreStore + LoadStore barrier (producer writes)
     *   - Volatile:     full fence (slowest, we avoid this)
     *
     * WHY NOT just use `volatile`?
     * `volatile` in Java is a FULL memory fence (like std::memory_order_seq_cst).
     * For SPSC, we only need acquire/release semantics — half the cost.
     */
    private static final VarHandle BYTE_BUFFER_LONG_VIEW;
    private static final VarHandle BYTE_BUFFER_BYTE_VIEW;

    static {
        // Create VarHandles that can read/write longs and bytes from a ByteBuffer
        // with explicit memory ordering control
        BYTE_BUFFER_LONG_VIEW = MethodHandles.byteBufferViewVarHandle(
                long[].class, ByteOrder.nativeOrder());
        BYTE_BUFFER_BYTE_VIEW = MethodHandles.byteBufferViewVarHandle(
                byte[].class, ByteOrder.nativeOrder());
    }

    // ── Instance Fields ─────────────────────────────────────────────────
    private final MappedByteBuffer buffer;
    private final FileChannel channel;
    private final int capacity;       // number of message slots
    private final int slotSize;       // bytes per slot (length + payload + commit)
    private final int ringSize;       // total bytes in the data region

    /**
     * Creates a new SPSC Ring Buffer backed by a memory-mapped file.
     *
     * @param filePath     path to the shared file (created if absent)
     * @param capacity     number of message slots (must be power of 2)
     * @param maxPayloadSize maximum payload size per message in bytes
     * @throws IOException if file mapping fails
     */
    public SPSCRingBuffer(Path filePath, int capacity, int maxPayloadSize) throws IOException {
        if (Integer.bitCount(capacity) != 1) {
            throw new IllegalArgumentException(
                    "Capacity must be a power of 2, got: " + capacity
                            + ". Use " + Integer.highestOneBit(capacity) * 2 + " instead.");
        }

        this.capacity = capacity;
        this.slotSize = LENGTH_FIELD_SIZE + maxPayloadSize + COMMIT_FLAG_SIZE;
        this.ringSize = capacity * slotSize;

        long totalFileSize = DATA_OFFSET + ringSize;

        this.channel = FileChannel.open(filePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);

        this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, totalFileSize);
        this.buffer.order(ByteOrder.nativeOrder());
    }

    // ══════════════════════════════════════════════════════════════════════
    //  PRODUCER API
    // ══════════════════════════════════════════════════════════════════════

    /**
     * Write a message into the ring buffer.
     *
     * Memory ordering contract:
     *   1. Write payload bytes                   (plain stores)
     *   2. Write payload length                  (plain store)
     *   3. Write commit flag = 1                 (RELEASE store)
     *   4. Advance producer sequence             (RELEASE store)
     *
     * The RELEASE on step 3 guarantees that ALL previous writes (payload + length)
     * are visible to ANY thread that sees commit == 1 via an ACQUIRE load.
     *
     * @param payload the message bytes to write
     * @return true if written, false if the buffer is full
     */
    public boolean write(byte[] payload) {
        long producerSeq = getProducerSequence();
        long consumerSeq = getConsumerSequenceAcquire();

        // Check if buffer is full (producer has lapped the consumer)
        if (producerSeq - consumerSeq >= capacity) {
            return false; // Buffer full — don't block, let caller decide
        }

        int slotIndex = (int) (producerSeq & (capacity - 1)); // mod via bitmask
        int slotOffset = DATA_OFFSET + (slotIndex * slotSize);

        // Step 1: Write payload length
        buffer.putLong(slotOffset, payload.length);

        // Step 2: Write payload data
        for (int i = 0; i < payload.length; i++) {
            buffer.put(slotOffset + LENGTH_FIELD_SIZE + i, payload[i]);
        }

        // Step 3: Write commit flag with RELEASE semantics
        // This is the "memory barrier" — it guarantees all previous writes
        // (length + payload) are flushed to memory BEFORE the commit flag.
        int commitOffset = slotOffset + LENGTH_FIELD_SIZE + (slotSize - LENGTH_FIELD_SIZE - COMMIT_FLAG_SIZE);
        setByteRelease(buffer, commitOffset, (byte) 1);

        // Step 4: Advance producer sequence with RELEASE semantics
        setProducerSequenceRelease(producerSeq + 1);

        return true;
    }

    // ══════════════════════════════════════════════════════════════════════
    //  CONSUMER API
    // ══════════════════════════════════════════════════════════════════════

    /**
     * Read the next message from the ring buffer into a pre-allocated MessageView.
     *
     * Memory ordering contract:
     *   1. Read producer sequence                (ACQUIRE load)
     *   2. Check if data available
     *   3. Read commit flag                      (ACQUIRE load)
     *   4. Read payload length + data            (plain loads — safe after ACQUIRE)
     *   5. Clear commit flag                     (plain store)
     *   6. Advance consumer sequence             (RELEASE store)
     *
     * The ACQUIRE on step 3 guarantees that if we see commit == 1,
     * we are guaranteed to see the payload + length the producer wrote.
     *
     * @param view a reusable MessageView (Flyweight — zero allocation!)
     * @return true if a message was read, false if buffer is empty
     */
    public boolean read(MessageView view) {
        long consumerSeq = getConsumerSequence();
        long producerSeq = getProducerSequenceAcquire();

        // Nothing to read
        if (consumerSeq >= producerSeq) {
            return false;
        }

        int slotIndex = (int) (consumerSeq & (capacity - 1));
        int slotOffset = DATA_OFFSET + (slotIndex * slotSize);

        // Step 3: Check commit flag with ACQUIRE semantics
        int commitOffset = slotOffset + LENGTH_FIELD_SIZE + (slotSize - LENGTH_FIELD_SIZE - COMMIT_FLAG_SIZE);
        byte committed = getByteAcquire(buffer, commitOffset);

        if (committed != 1) {
            return false; // Producer hasn't finished writing yet
        }

        // Step 4: Read message into the flyweight view (ZERO allocation)
        view.wrap(buffer, slotOffset, slotSize);

        // Step 5: Clear commit flag for reuse
        buffer.put(commitOffset, (byte) 0);

        // Step 6: Advance consumer sequence
        setConsumerSequenceRelease(consumerSeq + 1);

        return true;
    }

    // ══════════════════════════════════════════════════════════════════════
    //  VARHANDLE MEMORY BARRIER OPERATIONS
    // ══════════════════════════════════════════════════════════════════════

    /*
     * These methods wrap the VarHandle calls for readability.
     *
     * ACQUIRE = "I need to see everything the other thread wrote BEFORE this value."
     *   → Used by the READER (consumer) of a shared variable.
     *   → Equivalent to C++ std::memory_order_acquire
     *
     * RELEASE = "Everything I wrote BEFORE this must be visible to whoever reads this."
     *   → Used by the WRITER (producer) of a shared variable.
     *   → Equivalent to C++ std::memory_order_release
     *
     * Together they form an ACQUIRE-RELEASE pair — the minimum synchronization
     * needed for correct lock-free SPSC communication.
     */

    private long getProducerSequence() {
        return (long) BYTE_BUFFER_LONG_VIEW.get(buffer, PRODUCER_SEQUENCE_OFFSET);
    }

    private long getProducerSequenceAcquire() {
        return (long) BYTE_BUFFER_LONG_VIEW.getAcquire(buffer, PRODUCER_SEQUENCE_OFFSET);
    }

    private void setProducerSequenceRelease(long value) {
        BYTE_BUFFER_LONG_VIEW.setRelease(buffer, PRODUCER_SEQUENCE_OFFSET, value);
    }

    private long getConsumerSequence() {
        return (long) BYTE_BUFFER_LONG_VIEW.get(buffer, CONSUMER_SEQUENCE_OFFSET);
    }

    private long getConsumerSequenceAcquire() {
        return (long) BYTE_BUFFER_LONG_VIEW.getAcquire(buffer, CONSUMER_SEQUENCE_OFFSET);
    }

    private void setConsumerSequenceRelease(long value) {
        BYTE_BUFFER_LONG_VIEW.setRelease(buffer, CONSUMER_SEQUENCE_OFFSET, value);
    }

    private static byte getByteAcquire(ByteBuffer buf, int offset) {
        return (byte) BYTE_BUFFER_BYTE_VIEW.getAcquire(buf, offset);
    }

    private static void setByteRelease(ByteBuffer buf, int offset, byte value) {
        BYTE_BUFFER_BYTE_VIEW.setRelease(buf, offset, value);
    }

    // ══════════════════════════════════════════════════════════════════════
    //  UTILITY
    // ══════════════════════════════════════════════════════════════════════

    /** Reset all sequences and clear the buffer. Useful for benchmarking. */
    public void reset() {
        BYTE_BUFFER_LONG_VIEW.setVolatile(buffer, PRODUCER_SEQUENCE_OFFSET, 0L);
        BYTE_BUFFER_LONG_VIEW.setVolatile(buffer, CONSUMER_SEQUENCE_OFFSET, 0L);
        for (int i = DATA_OFFSET; i < DATA_OFFSET + ringSize; i++) {
            buffer.put(i, (byte) 0);
        }
    }

    public int getCapacity() {
        return capacity;
    }

    @Override
    public void close() throws IOException {
        // Force flush to disk (optional — OS handles this on crash too)
        buffer.force();
        channel.close();
    }
}
