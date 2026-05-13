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
 * Lock-free SPSC ring buffer over a memory-mapped file.
 *
 * File layout:
 *   [0..7]     producer sequence
 *   [8..63]    padding (avoid false sharing)
 *   [64..71]   consumer sequence
 *   [72..127]  padding
 *   [128..]    ring buffer slots
 *
 * Each slot: [8B length][payload][1B commit flag]
 *
 * The padding keeps producer and consumer pointers on separate cache lines
 * so the two CPUs don't fight over the same 64-byte chunk. Without it,
 * throughput drops roughly 10x on most hardware.
 */
public class SPSCRingBuffer implements AutoCloseable {

    private static final int CACHE_LINE = 64;

    // header offsets — each sequence gets its own cache line
    static final int PRODUCER_SEQ_OFF = 0;
    static final int CONSUMER_SEQ_OFF = CACHE_LINE;       // 64
    static final int DATA_OFF         = CACHE_LINE * 2;   // 128

    // per-slot field sizes
    static final int LEN_SIZE    = Long.BYTES;  // 8
    static final int COMMIT_SIZE = Byte.BYTES;  // 1

    // VarHandles — java's version of std::atomic, but for ByteBuffers.
    // lets us pick acquire/release instead of paying for a full volatile fence.
    private static final VarHandle LONG_VH;
    private static final VarHandle BYTE_VH;

    static {
        LONG_VH = MethodHandles.byteBufferViewVarHandle(long[].class, ByteOrder.nativeOrder());
        BYTE_VH = MethodHandles.byteBufferViewVarHandle(byte[].class, ByteOrder.nativeOrder());
    }

    private final MappedByteBuffer buffer;
    private final FileChannel channel;
    private final int capacity;
    private final int maxPayloadSize;
    private final int slotSize;
    private final int ringSize;

    // local caches so we don't cross cache lines on every call.
    // only refresh when we actually need to (buffer looks full/empty).
    // same trick the disruptor uses ("cachedGatingSequence").
    private long cachedConsumerSeq = 0;
    private long cachedProducerSeq = 0;

    public SPSCRingBuffer(Path filePath, int capacity, int maxPayloadSize) throws IOException {
        if (Integer.bitCount(capacity) != 1) {
            throw new IllegalArgumentException(
                    "Capacity must be a power of 2, got: " + capacity
                            + ". Use " + Integer.highestOneBit(capacity) * 2 + " instead.");
        }
        if (maxPayloadSize <= 0) {
            throw new IllegalArgumentException("maxPayloadSize must be positive, got: " + maxPayloadSize);
        }

        this.capacity = capacity;
        this.maxPayloadSize = maxPayloadSize;
        this.slotSize = LEN_SIZE + maxPayloadSize + COMMIT_SIZE;
        this.ringSize = capacity * slotSize;

        long totalSize = DATA_OFF + ringSize;

        this.channel = FileChannel.open(filePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);

        this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, totalSize);
        this.buffer.order(ByteOrder.nativeOrder());
    }

    // --- producer ---

    /**
     * Try to write a message. Returns false if the buffer is full.
     *
     * Ordering: we write length + payload with plain stores, then set the
     * commit flag with release semantics. That guarantees the consumer
     * sees fully-written data when it reads commit=1 via acquire.
     */
    public boolean write(byte[] payload) {
        if (payload.length > maxPayloadSize) {
            throw new IllegalArgumentException(
                    "Payload size " + payload.length + " exceeds max " + maxPayloadSize);
        }

        long pSeq = prodSeq();

        // check local cache first — avoids a cross-cache-line read most of the time
        if (pSeq - cachedConsumerSeq >= capacity) {
            cachedConsumerSeq = consSeqAcquire();
            if (pSeq - cachedConsumerSeq >= capacity) {
                return false; // actually full
            }
        }

        int slot = (int) (pSeq & (capacity - 1));
        int off  = DATA_OFF + (slot * slotSize);

        buffer.putLong(off, payload.length);

        int payOff = off + LEN_SIZE;
        for (int i = 0; i < payload.length; i++) {
            buffer.put(payOff + i, payload[i]);
        }

        // release barrier — everything above is visible before commit=1
        int commitOff = off + LEN_SIZE + maxPayloadSize;
        setByteRelease(buffer, commitOff, (byte) 1);

        setProdSeqRelease(pSeq + 1);
        return true;
    }

    /** Same as write(byte[]) but reads from a ByteBuffer. Consumes src fully. */
    public boolean write(ByteBuffer src) {
        int len = src.remaining();
        if (len > maxPayloadSize) {
            throw new IllegalArgumentException(
                    "Payload size " + len + " exceeds max " + maxPayloadSize);
        }

        long pSeq = prodSeq();

        if (pSeq - cachedConsumerSeq >= capacity) {
            cachedConsumerSeq = consSeqAcquire();
            if (pSeq - cachedConsumerSeq >= capacity) {
                return false;
            }
        }

        int slot = (int) (pSeq & (capacity - 1));
        int off  = DATA_OFF + (slot * slotSize);

        buffer.putLong(off, len);
        int payOff = off + LEN_SIZE;
        for (int i = 0; i < len; i++) {
            buffer.put(payOff + i, src.get());
        }

        int commitOff = off + LEN_SIZE + maxPayloadSize;
        setByteRelease(buffer, commitOff, (byte) 1);
        setProdSeqRelease(pSeq + 1);
        return true;
    }

    // --- consumer ---

    /**
     * Try to read the next message into a reusable view. Returns false if empty.
     *
     * Ordering: we acquire-load the commit flag. If it's 1, the release on
     * the producer side guarantees all the payload writes are visible to us.
     */
    public boolean read(MessageView view) {
        long cSeq = consSeq();

        if (cSeq >= cachedProducerSeq) {
            cachedProducerSeq = prodSeqAcquire();
            if (cSeq >= cachedProducerSeq) {
                return false;
            }
        }

        int slot = (int) (cSeq & (capacity - 1));
        int off  = DATA_OFF + (slot * slotSize);

        int commitOff = off + LEN_SIZE + maxPayloadSize;
        byte committed = getByteAcquire(buffer, commitOff);
        if (committed != 1) {
            return false; // producer is mid-write
        }

        view.wrap(buffer, off, slotSize, maxPayloadSize);

        buffer.put(commitOff, (byte) 0); // clear for reuse
        setConsSeqRelease(cSeq + 1);
        return true;
    }

    // --- VarHandle wrappers ---
    // just thin wrappers so the call sites read cleaner.
    // acquire = "show me everything written before this"  (consumer side)
    // release = "flush everything I wrote before this"    (producer side)

    private long prodSeq()            { return (long) LONG_VH.get(buffer, PRODUCER_SEQ_OFF); }
    private long prodSeqAcquire()     { return (long) LONG_VH.getAcquire(buffer, PRODUCER_SEQ_OFF); }
    private void setProdSeqRelease(long v) { LONG_VH.setRelease(buffer, PRODUCER_SEQ_OFF, v); }

    private long consSeq()            { return (long) LONG_VH.get(buffer, CONSUMER_SEQ_OFF); }
    private long consSeqAcquire()     { return (long) LONG_VH.getAcquire(buffer, CONSUMER_SEQ_OFF); }
    private void setConsSeqRelease(long v) { LONG_VH.setRelease(buffer, CONSUMER_SEQ_OFF, v); }

    private static byte getByteAcquire(ByteBuffer b, int off) {
        return (byte) BYTE_VH.getAcquire(b, off);
    }
    private static void setByteRelease(ByteBuffer b, int off, byte v) {
        BYTE_VH.setRelease(b, off, v);
    }

    // --- util ---

    /** Zeros everything out. Call before benchmarks or on startup. */
    public void reset() {
        LONG_VH.setVolatile(buffer, PRODUCER_SEQ_OFF, 0L);
        LONG_VH.setVolatile(buffer, CONSUMER_SEQ_OFF, 0L);
        cachedConsumerSeq = 0;
        cachedProducerSeq = 0;
        for (int i = DATA_OFF; i < DATA_OFF + ringSize; i++) {
            buffer.put(i, (byte) 0);
        }
    }

    public int getCapacity()       { return capacity; }
    public int getMaxPayloadSize() { return maxPayloadSize; }
    public int getSlotSize()       { return slotSize; }

    /** Approximate — reads are racy since producer/consumer run on different threads. */
    public long size() {
        long p = (long) LONG_VH.getAcquire(buffer, PRODUCER_SEQ_OFF);
        long c = (long) LONG_VH.getAcquire(buffer, CONSUMER_SEQ_OFF);
        return p - c;
    }

    public long remainingCapacity() { return capacity - size(); }

    @Override
    public void close() throws IOException {
        buffer.force();
        channel.close();
    }

    @Override
    public String toString() {
        return "SPSCRingBuffer{capacity=" + capacity
                + ", maxPayload=" + maxPayloadSize
                + ", slotSize=" + slotSize
                + ", size~" + size() + "}";
    }
}
